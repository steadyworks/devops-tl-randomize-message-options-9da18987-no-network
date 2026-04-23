import asyncio
import logging
import shutil
from pathlib import Path
from uuid import UUID

from backend.db.dal import (
    DALAssets,
    DALPhotobooks,
    DAOAssetsCreate,
    DAOAssetsUpdate,
    DAOPhotobooksUpdate,
    safe_commit,
)
from backend.db.data_models import AssetUploadStatus, PhotobookStatus
from backend.db.session.factory import AsyncSessionFactory
from backend.lib.asset_manager.base import AssetManager
from backend.lib.types.asset import Asset, AssetStorageKey
from backend.lib.utils.common import none_throws
from backend.worker.process.types import LocalCPUBoundWorkerProcessResources

from .local import LocalJobProcessor
from .types import (
    AssetCompressUploadInputPayload,
    AssetCompressUploadOutputPayload,
    JobType,
    PhotobookGenerationInputPayload,
)
from .utils.compress_upload_mixin import (
    AssetKeyType,
    CompressUploadMixin,
    compression_tier_tempdir,
)
from .utils.types import CompressionTier
from .utils.vips import ImageProcessingLibrary


# No longer needed. Moved to remote post process uploaded assets
class LocalAssetCompressUploadJobProcessorDEPRECATED(
    LocalJobProcessor[
        AssetCompressUploadInputPayload,
        AssetCompressUploadOutputPayload,
        LocalCPUBoundWorkerProcessResources,
    ],
    CompressUploadMixin,
):
    def __init__(
        self,
        job_id: UUID,
        asset_manager: AssetManager,
        db_session_factory: AsyncSessionFactory,
        worker_process_resources: LocalCPUBoundWorkerProcessResources,
    ) -> None:
        super().__init__(
            job_id,
            asset_manager,
            db_session_factory,
            worker_process_resources,
        )
        self._image_lib = ImageProcessingLibrary(
            max_concurrent=1,
        )

    def _sanity_check_paths_and_free_storage(
        self, input_payload: AssetCompressUploadInputPayload
    ) -> tuple[bool, str]:  # (should_abort, error_message)
        should_abort, error_message = False, ""
        # Sanity check that all image paths and root dir exists
        missing_paths = [
            p for p in input_payload.absolute_media_paths if not p.is_file()
        ]
        if not input_payload.root_tempdir.is_dir():
            should_abort = True
            error_message = (
                "[LocalAssetCompressUploadJobProcessor] Temp output directory does not exist: "
                f"{input_payload.root_tempdir}"
            )
        if missing_paths:
            should_abort = True
            error_message = (
                "[LocalAssetCompressUploadJobProcessor] Missing input files:"
                f"{', '.join(str(p) for p in missing_paths)}"
            )

        # Sanity check that we have enough spare disk space for compression
        should_abort_free_storage, error_message_free_storage = (
            self._sanity_check_free_storage(input_payload.root_tempdir)
        )

        return (
            should_abort or should_abort_free_storage,
            error_message or error_message_free_storage,
        )

    @classmethod
    async def _upload_to_asset_storage_persisting_metadata(
        cls,
        media_paths_asset_uuids_map: dict[Path, UUID],
        media_paths_compressed_paths_map: dict[Path, Path],
        originating_photobook_id: UUID,
        asset_key_type: AssetKeyType,
        asset_manager: AssetManager,
        db_session_factory: AsyncSessionFactory,
    ) -> None:
        compressed_paths_orig_paths_map = {
            v: k for k, v in media_paths_compressed_paths_map.items()
        }

        upload_inputs: list[tuple[Path, AssetStorageKey]] = []
        for orig_local_path in media_paths_asset_uuids_map.keys():
            if orig_local_path not in media_paths_compressed_paths_map:
                continue
            compressed_local_path = media_paths_compressed_paths_map[orig_local_path]
            upload_inputs.append(
                (
                    compressed_local_path,
                    asset_manager.mint_asset_key(
                        originating_photobook_id, compressed_local_path.name
                    ),
                )
            )
        upload_results = await asset_manager.upload_files_batched(upload_inputs)

        dao_updates: dict[UUID, DAOAssetsUpdate] = dict()
        for compressed_local_path, upload_res in upload_results.items():
            if not isinstance(upload_res, Asset):
                continue
            if compressed_local_path not in compressed_paths_orig_paths_map:
                continue
            local_orig_path = compressed_paths_orig_paths_map[compressed_local_path]
            asset_uuid = media_paths_asset_uuids_map.get(local_orig_path)
            if asset_uuid is None:
                continue
            update_obj = None
            if asset_key_type == "asset_key_original":
                update_obj = DAOAssetsUpdate(
                    asset_key_original=upload_res.asset_storage_key
                )
            elif asset_key_type == "asset_key_display":
                update_obj = DAOAssetsUpdate(
                    asset_key_display=upload_res.asset_storage_key
                )
            elif asset_key_type == "asset_key_llm":
                update_obj = DAOAssetsUpdate(asset_key_llm=upload_res.asset_storage_key)
            else:
                pass
            if update_obj is None:
                continue

            dao_updates[asset_uuid] = update_obj

        if not dao_updates:
            logging.warning(
                "[LocalAssetCompressUploadJobProcessor] no asset uploads to persist"
            )
            if asset_key_type == "asset_key_original":
                raise Exception("No original assets were uploaded")

        async with db_session_factory.new_session() as db_session:
            async with safe_commit(
                db_session,
                context="persist uploaded asset storage keys",
                raise_on_fail=asset_key_type == "asset_key_original",
            ):
                await DALAssets.update_many_by_ids(db_session, dao_updates)

    @classmethod
    async def _compress_and_upload_safe(
        cls,
        image_lib: ImageProcessingLibrary,
        tier: CompressionTier,
        media_paths_asset_uuids_map: dict[Path, UUID],
        root_path: Path,
        originating_photobook_id: UUID,
        asset_manager: AssetManager,
        db_session_factory: AsyncSessionFactory,
    ) -> None:
        try:
            with compression_tier_tempdir(tier, root_path) as root_compress_tempdir:
                result = await image_lib.compress_by_tier_on_thread(
                    input_paths=list(media_paths_asset_uuids_map.keys()),
                    output_dir=root_compress_tempdir,
                    format="jpeg",
                    tier=tier,
                    strip_metadata=False,
                )

                # Upload assets where compressions are successful
                media_paths_compressed_paths_map: dict[Path, Path] = dict()
                for original_path, res in result.items():
                    if res.is_compress_succeeded and res.compressed_path is not None:
                        media_paths_compressed_paths_map[original_path] = (
                            res.compressed_path
                        )

                await cls._upload_to_asset_storage_persisting_metadata(
                    media_paths_asset_uuids_map,
                    media_paths_compressed_paths_map,
                    originating_photobook_id,
                    cls._get_asset_key_type_by_compression_tier(tier),
                    asset_manager,
                    db_session_factory,
                )
        except Exception:
            logging.warning(
                f"[LocalAssetCompressUploadJobProcessor] Upload failed "
                f"for tier {tier}, photobook_id {originating_photobook_id}"
            )

    async def process(
        self, input_payload: AssetCompressUploadInputPayload
    ) -> AssetCompressUploadOutputPayload:
        try:
            # 1. Sanity check
            should_abort, error_message = self._sanity_check_paths_and_free_storage(
                input_payload
            )
            if should_abort:
                async with self.db_session_factory.new_session() as db_session:
                    async with safe_commit(
                        db_session,
                        context="upload failed status DB update",
                        raise_on_fail=True,
                    ):
                        await DALPhotobooks.update_by_id(
                            db_session,
                            none_throws(input_payload.originating_photobook_id),
                            DAOPhotobooksUpdate(
                                status=PhotobookStatus.UPLOAD_FAILED,
                            ),
                        )
                raise FileNotFoundError(error_message)

            # Begin compression and upload
            # 2. Insert initial objects
            media_paths_asset_uuids_map: dict[Path, UUID] = dict()
            async with self.db_session_factory.new_session() as db_session:
                async with safe_commit(
                    db_session,
                    context="photobook asset compression and upload status DB update",
                    raise_on_fail=False,
                ):
                    await DALPhotobooks.update_by_id(
                        db_session,
                        none_throws(input_payload.originating_photobook_id),
                        DAOPhotobooksUpdate(
                            status=PhotobookStatus.UPLOADING,
                        ),
                    )

                async with safe_commit(
                    db_session,
                    context="initializing asset objects",
                    raise_on_fail=True,
                ):
                    dao_creates: list[DAOAssetsCreate] = []
                    for _idx in range(len(input_payload.absolute_media_paths)):
                        dao_creates.append(
                            DAOAssetsCreate(
                                user_id=input_payload.user_id,
                                asset_key_original=None,
                                asset_key_display=None,
                                asset_key_llm=None,
                                metadata_json=None,
                                original_photobook_id=none_throws(
                                    input_payload.originating_photobook_id
                                ),
                                upload_status=AssetUploadStatus.PENDING,
                            )
                        )
                    daos = await DALAssets.create_many(db_session, dao_creates)

                for media_path, dao in zip(input_payload.absolute_media_paths, daos):
                    media_paths_asset_uuids_map[media_path] = dao.id

            # Step 3: launch LLM-level photo quality compression tasks first
            await self._compress_and_upload_safe(
                image_lib=self._image_lib,
                tier=CompressionTier.LLM,
                media_paths_asset_uuids_map=media_paths_asset_uuids_map,
                root_path=input_payload.root_tempdir,
                originating_photobook_id=none_throws(
                    input_payload.originating_photobook_id
                ),
                asset_manager=self.asset_manager,
                db_session_factory=self.db_session_factory,
            )

            # Step 4: Enqueue job for LLM creation
            async with self.db_session_factory.new_session() as db_session:
                enqueued_remote_job_id = await self.worker_process_resources.remote_io_bound_job_manager.enqueue(
                    job_type=JobType.REMOTE_PHOTOBOOK_GENERATION,
                    job_payload=PhotobookGenerationInputPayload(
                        user_id=input_payload.user_id,
                        originating_photobook_id=none_throws(
                            input_payload.originating_photobook_id
                        ),
                        asset_ids=[
                            asset_id
                            for asset_id in media_paths_asset_uuids_map.values()
                        ],
                    ),
                    max_retries=2,
                    db_session=db_session,
                )

            # Step 5: In parallel compress, and upload displayed version + original copies
            upload_original_copies_task = asyncio.create_task(
                self._upload_to_asset_storage_persisting_metadata(
                    media_paths_asset_uuids_map=media_paths_asset_uuids_map,
                    media_paths_compressed_paths_map={  # No compression, identical with media_paths_asset_uuids_map
                        k: k for k in media_paths_asset_uuids_map.keys()
                    },
                    originating_photobook_id=none_throws(
                        input_payload.originating_photobook_id
                    ),
                    asset_key_type="asset_key_original",
                    asset_manager=self.asset_manager,
                    db_session_factory=self.db_session_factory,
                )
            )
            highend_display_compress_upload_task = asyncio.create_task(
                self._compress_and_upload_safe(
                    image_lib=self._image_lib,
                    tier=CompressionTier.HIGH_END_DISPLAY,
                    media_paths_asset_uuids_map=media_paths_asset_uuids_map,
                    root_path=input_payload.root_tempdir,
                    originating_photobook_id=none_throws(
                        input_payload.originating_photobook_id
                    ),
                    asset_manager=self.asset_manager,
                    db_session_factory=self.db_session_factory,
                )
            )

            upload_orig, _upload_highend_display = await asyncio.gather(
                upload_original_copies_task,
                highend_display_compress_upload_task,
                return_exceptions=True,
            )
            if isinstance(upload_orig, Exception):
                # Permanent failure
                async with self.db_session_factory.new_session() as db_session:
                    async with safe_commit(
                        db_session,
                        context="upload failed status DB update",
                        raise_on_fail=True,
                    ):
                        await DALPhotobooks.update_by_id(
                            db_session,
                            none_throws(input_payload.originating_photobook_id),
                            DAOPhotobooksUpdate(
                                status=PhotobookStatus.UPLOAD_FAILED,
                            ),
                        )

            return AssetCompressUploadOutputPayload(
                job_id=self.job_id,
                enqueued_photobook_creation_remote_job_id=enqueued_remote_job_id,
            )
        finally:
            # Step 6: cleanup tempdir
            try:
                shutil.rmtree(input_payload.root_tempdir, ignore_errors=True)
            except Exception as e:
                logging.warning(
                    f"[LocalAssetCompressUploadJobProcessor] Failed to clean up "
                    f"tempdir {input_payload.root_tempdir}: {e}"
                )
