from collections import defaultdict

from django.db import IntegrityError, transaction
from django.db.models import Q

from pulpcore.plugin.models import ContentArtifact, RemoteArtifact

from .api import Stage


class QueryExistingContentUnits(Stage):
    """
    A Stages API stage that saves :attr:`DeclarativeContent.content` objects and saves its related
    :class:`~pulpcore.plugin.models.ContentArtifact` objects too.

    This stage expects :class:`~pulpcore.plugin.stages.DeclarativeContent` units from `in_q` and
    inspects their associated :class:`~pulpcore.plugin.stages.DeclarativeArtifact` objects. Each
    :class:`~pulpcore.plugin.stages.DeclarativeArtifact` object stores one
    :class:`~pulpcore.plugin.models.Artifact`.

    This stage inspects any "unsaved" Content unit objects and searches for existing saved Content
    units inside Pulp with the same unit key. Any existing Content objects found, replace their
    "unsaved" counterpart in the :class:`~pulpcore.plugin.stages.DeclarativeContent` object.

    Each :class:`~pulpcore.plugin.stages.DeclarativeContent` is sent to `out_q` after it has been
    handled.

    This stage drains all available items from `in_q` and batches everything into one large call to
    the db for efficiency.
    """

    async def __call__(self, in_q, out_q):
        """
        The coroutine for this stage.

        Args:
            in_q (:class:`asyncio.Queue`): The queue to receive
                :class:`~pulpcore.plugin.stages.DeclarativeContent` objects from.
            out_q (:class:`asyncio.Queue`): The queue to put
                :class:`~pulpcore.plugin.stages.DeclarativeContent` into.

        Returns:
            The coroutine for this stage.
        """
        async for batch in self.batches(in_q):
            content_q_by_type = defaultdict(lambda: Q(pk=None))
            for declarative_content in batch:
                model_type = type(declarative_content.content)
                unit_q = declarative_content.content.q()
                content_q_by_type[model_type] = content_q_by_type[model_type] | unit_q

            for model_type in content_q_by_type.keys():
                for result in model_type.objects.filter(content_q_by_type[model_type]):
                    for declarative_content in batch:
                        if type(declarative_content.content) is not model_type:
                            continue
                        not_same_unit = False
                        for field in result.natural_key_fields():
                            in_memory_digest_value = getattr(declarative_content.content, field)
                            if in_memory_digest_value != getattr(result, field):
                                not_same_unit = True
                                break
                        if not_same_unit:
                            continue
                        declarative_content.content = result
            for declarative_content in batch:
                await out_q.put(declarative_content)
        await out_q.put(None)


class ContentUnitSaver(Stage):
    """
    A Stages API stage that saves :attr:`DeclarativeContent.content` objects and saves its related
    :class:`~pulpcore.plugin.models.ContentArtifact` objects too.

    This stage expects :class:`~pulpcore.plugin.stages.DeclarativeContent` units from `in_q` and
    inspects their associated :class:`~pulpcore.plugin.stages.DeclarativeArtifact` objects. Each
    :class:`~pulpcore.plugin.stages.DeclarativeArtifact` object stores one
    :class:`~pulpcore.plugin.models.Artifact`.

    Each "unsaved" Content objects is saved and a :class:`~pulpcore.plugin.models.ContentArtifact`
    objects too.

    Each :class:`~pulpcore.plugin.stages.DeclarativeContent` is sent to after it has been handled.

    This stage drains all available items from `in_q` and batches everything into one large call to
    the db for efficiency.
    """

    async def __call__(self, in_q, out_q):
        """
        The coroutine for this stage.

        Args:
            in_q (:class:`asyncio.Queue`): The queue to receive
                :class:`~pulpcore.plugin.stages.DeclarativeContent` objects from.
            out_q (:class:`asyncio.Queue`): The queue to put
                :class:`~pulpcore.plugin.stages.DeclarativeContent` into.

        Returns:
            The coroutine for this stage.
        """
        async for batch in self.batches(in_q):
            content_artifact_bulk = []
            with transaction.atomic():
                await self._pre_save(batch)
                for declarative_content in batch:
                    if declarative_content.content.pk is None:
                        try:
                            with transaction.atomic():
                                declarative_content.content.save()
                        except IntegrityError:
                            declarative_content.content = \
                                declarative_content.content.__class__.objects.get(
                                    declarative_content.content.q())
                            continue
                        for declarative_artifact in declarative_content.d_artifacts:
                            content_artifact = ContentArtifact(
                                content=declarative_content.content,
                                artifact=declarative_artifact.artifact,
                                relative_path=declarative_artifact.relative_path
                            )
                            content_artifact_bulk.append(content_artifact)
                ContentArtifact.objects.bulk_get_or_create(content_artifact_bulk)
                await self._post_save(batch)
            for declarative_content in batch:
                await out_q.put(declarative_content)
        await out_q.put(None)

    async def _pre_save(self, batch):
        """
        A hook plugin-writers can override to save related objects prior to content unit saving.

        This is run within the same transaction as the content unit saving.

        Args:
            batch (list of :class:`~pulpcore.plugin.stages.DeclarativeContent`): The batch of
                :class:`~pulpcore.plugin.stages.DeclarativeContent` objects to be saved.

        """
        pass

    async def _post_save(self, batch):
        """
        A hook plugin-writers can override to save related objects after content unit saving.

        This is run within the same transaction as the content unit saving.

        Args:
            batch (list of :class:`~pulpcore.plugin.stages.DeclarativeContent`): The batch of
                :class:`~pulpcore.plugin.stages.DeclarativeContent` objects to be saved.

        """
        pass


class ResolveContentFutures(Stage):
    """
    This stage resolves the futures in :class:`~pulpcore.plugin.stages.DeclarativeContent`.

    Futures results are set to the found/created :class:`~pulpcore.plugin.models.Content`.
    """

    async def __call__(self, in_q, out_q):
        """
        The coroutine for this stage.

        Args:
            in_q (:class:`asyncio.Queue`): The queue to receive
                :class:`~pulpcore.plugin.stages.DeclarativeContent` objects from.
            out_q (:class:`asyncio.Queue`): The queue to put
                :class:`~pulpcore.plugin.stages.DeclarativeContent` into.

        Returns:
            The coroutine for this stage.
        """
        while True:
            d_content = await in_q.get()
            if d_content is None:
                await out_q.put(None)
                break
            if d_content.future is not None:
                d_content.future.set_result(d_content.content)
            await out_q.put(d_content)


class RemoteArtifactSaver(Stage):
    """
    A Stage that saves :class:`~pulpcore.plugin.models.RemoteArtifact` objects

    This stage expects :class:`~pulpcore.plugin.stages.DeclarativeContent` units from `in_q`
    and inspects their related :class:`~pulpcore.plugin.stages.DeclarativeArtifact` objects.
    An :class:`~pulpcore.plugin.models.RemoteArtifact` object is saved for each
    :class:`~pulpcore.plugin.stages.DeclarativeArtifact`.

    Each :class:`~pulpcore.plugin.stages.DeclarativeContent` is sent to `out_q` after it has been
    handled.

    This stage drains all available items from `in_q` and batches everything into one large
    call to the db for efficiency.
    """

    @staticmethod
    def _declared_remote_artifacts(batch):
        """
        Build a generator of "declared" :class:`~pulpcore.plugin.models.RemoteArtifact` to
        be created for the batch.

        Each RemoteArtifact corresponds a :class:`~pulpcore.plugin.stages.DeclarativeArtifact`
        associated with a :class:`~pulpcore.plugin.stages.DeclarativeContent` in the batch.

        Args:
            batch (list): List of :class:`~pulpcore.plugin.stages.DeclarativeContent`.

        Returns:
            Iterable: Of :class:`~pulpcore.plugin.models.RemoteArtifact`.
        """
        artifact_mapping = {}
        for d_content in batch:
            for d_artifact in d_content.d_artifacts:
                key = (
                    d_content.content.pk,
                    d_artifact.relative_path
                )
                artifact_mapping[key] = d_artifact
        for content_artifact in ContentArtifact.objects.filter(
                content__in=(dc.content for dc in batch)):
            key = (
                content_artifact.content.pk,
                content_artifact.relative_path
            )
            d_artifact = artifact_mapping[key]
            remote_artifact = RemoteArtifact(
                url=d_artifact.url,
                size=d_artifact.artifact.size,
                md5=d_artifact.artifact.md5,
                sha1=d_artifact.artifact.sha1,
                sha224=d_artifact.artifact.sha224,
                sha256=d_artifact.artifact.sha256,
                sha384=d_artifact.artifact.sha384,
                sha512=d_artifact.artifact.sha512,
                content_artifact=content_artifact,
                remote=d_artifact.remote
            )
            yield remote_artifact

    def _needed_remote_artifacts(self, batch):
        """
        Build a generator of only :class:`~pulpcore.plugin.models.RemoteArtifact` that need
        to be created for the batch.

        Args:
            batch (list): List of :class:`~pulpcore.plugin.stages.DeclarativeContent`.

        Returns:
            Iterable: Of :class:`~pulpcore.plugin.models.RemoteArtifact`.
        """
        q = Q(pk=None)
        existing = set()
        for d_content in batch:
            for content_artifact in ContentArtifact.objects.filter(content=d_content.content):
                q |= Q(content_artifact=content_artifact,
                       remote__in=(a.remote for a in d_content.d_artifacts))
        for remote_artifact in RemoteArtifact.objects.filter(q):
            key = (
                remote_artifact.remote.pk,
                remote_artifact.content_artifact.pk
            )
            existing.add(key)
        for remote_artifact in self._declared_remote_artifacts(batch):
            key = (
                remote_artifact.remote.pk,
                remote_artifact.content_artifact.pk
            )
            if key in existing:
                continue
            yield remote_artifact

    async def __call__(self, in_q, out_q):
        """
        The coroutine for this stage.

        Args:
            in_q (:class:`asyncio.Queue`): The queue to receive
                :class:`~pulpcore.plugin.stages.DeclarativeContent` objects from.
            out_q (:class:`asyncio.Queue`): The queue to put
                :class:`~pulpcore.plugin.stages.DeclarativeContent` into.

        Returns:
            The coroutine for this stage.
        """
        async for batch in self.batches(in_q):
            RemoteArtifact.objects.bulk_get_or_create(self._needed_remote_artifacts(batch))
            for d_content in batch:
                await out_q.put(d_content)
            await out_q.put(None)
