"""Thread-safe registry mapping module targets to BaseStepExecutor instances."""

from __future__ import annotations

import threading

from atlas_core import require_not_empty

from atlas_orchestration.exceptions import (
    DuplicateExecutorRegistrationError,
    InvalidStepDefinitionError,
    UnknownModuleTargetError,
)
from atlas_orchestration.execution.executor import BaseStepExecutor


class StepExecutorRegistry:
    """Thread-safe registry mapping ``module_target`` identifiers to :class:`BaseStepExecutor`."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._executors: dict[str, BaseStepExecutor] = {}

    @staticmethod
    def _normalize_target(module_target: str) -> str:
        clean = module_target.strip().lower() if isinstance(module_target, str) else ""
        try:
            require_not_empty(clean, "module_target")
        except ValueError as exc:
            raise InvalidStepDefinitionError(str(exc)) from exc
        return clean

    def register(
        self,
        module_target: str,
        executor: BaseStepExecutor,
        *,
        overwrite: bool = False,
    ) -> None:
        """Register ``executor`` for ``module_target``.

        Raises
        ------
        InvalidStepDefinitionError
            If ``module_target`` is empty or ``executor`` is not a :class:`BaseStepExecutor`.
        DuplicateExecutorRegistrationError
            If ``module_target`` is already registered and ``overwrite`` is ``False``.
        """
        target = self._normalize_target(module_target)
        if not isinstance(executor, BaseStepExecutor):
            raise InvalidStepDefinitionError(
                f"Executor for target '{target}' must be an instance of BaseStepExecutor, "
                f"got {type(executor).__name__}."
            )

        with self._lock:
            if target in self._executors and not overwrite:
                raise DuplicateExecutorRegistrationError(
                    f"Executor for module_target '{target}' is already registered.",
                    module_target=target,
                )
            self._executors[target] = executor

    def resolve(self, module_target: str) -> BaseStepExecutor:
        """Resolve and return the :class:`BaseStepExecutor` registered for ``module_target``.

        Raises
        ------
        UnknownModuleTargetError
            If no executor is registered for ``module_target``.
        """
        clean = module_target.strip().lower() if isinstance(module_target, str) else ""
        if not clean:
            raise UnknownModuleTargetError(
                "Step module_target must be a non-empty string.",
                module_target=module_target,
            )

        with self._lock:
            executor = self._executors.get(clean)
            if executor is None:
                registered = sorted(self._executors.keys())
                raise UnknownModuleTargetError(
                    f"No step executor registered for module_target '{module_target}'. "
                    f"Registered targets: {registered}",
                    module_target=module_target,
                )
            return executor

    def get(self, module_target: str) -> BaseStepExecutor:
        """Alias for :meth:`resolve`."""
        return self.resolve(module_target)

    def has_executor(self, module_target: str) -> bool:
        """Return ``True`` if an executor is registered for ``module_target``."""
        clean = module_target.strip().lower() if isinstance(module_target, str) else ""
        if not clean:
            return False
        with self._lock:
            return clean in self._executors

    def unregister(self, module_target: str) -> None:
        """Remove the executor registered for ``module_target``.

        Raises
        ------
        UnknownModuleTargetError
            If ``module_target`` is not currently registered.
        """
        clean = module_target.strip().lower() if isinstance(module_target, str) else ""
        if not clean:
            raise UnknownModuleTargetError(
                "Step module_target must be a non-empty string.",
                module_target=module_target,
            )
        with self._lock:
            if clean not in self._executors:
                raise UnknownModuleTargetError(
                    f"No step executor registered for module_target '{module_target}'.",
                    module_target=module_target,
                )
            del self._executors[clean]

    def list_targets(self) -> list[str]:
        """Return a sorted list of all registered ``module_target`` identifiers."""
        with self._lock:
            return sorted(self._executors.keys())


__all__ = ["StepExecutorRegistry"]

