"""SQLAlchemy implementation of :class:`ArtifactRepository`."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from atlas_metadata.exceptions import DuplicateArtifactError
from atlas_metadata.identifiers.urn import URN
from atlas_metadata.models.artifact import ArtifactMetadata
from atlas_metadata.repositories.contracts import ArtifactRepository
from atlas_metadata.repositories.sqlalchemy.base import (
    SqlAlchemyRepositoryBase,
    translate_database_errors,
)
from atlas_metadata.repositories.sqlalchemy.mappings import (
    apply_artifact_to_record,
    record_to_artifact,
)
from atlas_metadata.repositories.sqlalchemy.models import ArtifactRecord


class SqlAlchemyArtifactRepository(SqlAlchemyRepositoryBase, ArtifactRepository):
    """Persists artifact metadata through SQLAlchemy.

    Artifacts are keyed by ``artifact_id`` / URN (never by ``dataset_id``), so each execution's
    artifact is a distinct row and ``overwrite=True`` only ever replaces the same artifact.
    """

    def save_artifact(
        self,
        artifact: ArtifactMetadata,
        *,
        overwrite: bool = False,
    ) -> ArtifactMetadata:
        urn = str(artifact.urn)
        try:
            with (
                translate_database_errors("save_artifact"),
                self._session_factory.begin() as session,
            ):
                existing = session.scalar(
                    select(ArtifactRecord).where(
                        ArtifactRecord.workspace_id == self._workspace_id,
                        ArtifactRecord.artifact_id == artifact.artifact_id,
                    )
                )
                urn_owner = session.scalar(
                    select(ArtifactRecord).where(
                        ArtifactRecord.workspace_id == self._workspace_id,
                        ArtifactRecord.urn == urn,
                    )
                )

                if (existing is not None or urn_owner is not None) and not overwrite:
                    raise self._duplicate_artifact(artifact)
                if existing is None and urn_owner is not None:
                    raise self._duplicate_artifact(artifact)

                if existing is None:
                    session.add(
                        apply_artifact_to_record(
                            ArtifactRecord(), artifact, workspace_id=self._workspace_id
                        )
                    )
                else:
                    apply_artifact_to_record(existing, artifact, workspace_id=self._workspace_id)
                session.flush()
        except IntegrityError as exc:
            raise self._duplicate_artifact(artifact) from exc
        return artifact

    def get_artifact(self, artifact_id: str) -> ArtifactMetadata | None:
        with translate_database_errors("get_artifact"), self._session_factory() as session:
            record = session.scalar(
                select(ArtifactRecord).where(
                    ArtifactRecord.workspace_id == self._workspace_id,
                    ArtifactRecord.artifact_id == artifact_id.strip(),
                )
            )
            return record_to_artifact(record) if record is not None else None

    def get_artifact_by_urn(self, urn: URN | str) -> ArtifactMetadata | None:
        parsed = URN.parse(urn)
        with translate_database_errors("get_artifact_by_urn"), self._session_factory() as session:
            record = session.scalar(
                select(ArtifactRecord).where(
                    ArtifactRecord.workspace_id == self._workspace_id,
                    ArtifactRecord.urn == str(parsed),
                )
            )
            return record_to_artifact(record) if record is not None else None

    def list_artifacts(
        self,
        *,
        dataset_id: str | None = None,
        execution_id: str | None = None,
    ) -> list[ArtifactMetadata]:
        statement = (
            select(ArtifactRecord)
            .where(ArtifactRecord.workspace_id == self._workspace_id)
            .order_by(ArtifactRecord.created_at, ArtifactRecord.artifact_id)
        )
        if dataset_id is not None:
            statement = statement.where(ArtifactRecord.dataset_id == dataset_id.strip())
        if execution_id is not None:
            statement = statement.where(ArtifactRecord.execution_id == execution_id.strip())

        with translate_database_errors("list_artifacts"), self._session_factory() as session:
            return [record_to_artifact(record) for record in session.scalars(statement)]

    @staticmethod
    def _duplicate_artifact(artifact: ArtifactMetadata) -> DuplicateArtifactError:
        return DuplicateArtifactError(
            f"Artifact '{artifact.artifact_id}' (URN '{artifact.urn}') is already registered.",
            urn=str(artifact.urn),
            dataset_id=artifact.dataset_id,
            artifact_id=artifact.artifact_id,
        )


__all__ = ["SqlAlchemyArtifactRepository"]
