"""Dynamic parameter and context reference resolver for atlas-orchestration."""

from __future__ import annotations

import copy
from typing import Any

from atlas_orchestration.exceptions import UnresolvedContextReferenceError
from atlas_orchestration.execution.context import ExecutionContext


class ContextResolver:
    """Resolves ``${...}`` context references inside workflow step parameters.

    Supported Reference Namespaces
    ------------------------------
    - ``${steps.<step_id>.<output_key>[.<nested_key>...]}`` -> ``context.step_outputs``
    - ``${variables.<var_name>[.<nested_key>...]}`` -> ``context.workflow_variables``
    - ``${workflow.<var_name>[.<nested_key>...]}`` -> ``context.workflow_variables``
    - ``${metadata.<meta_key>[.<nested_key>...]}`` -> ``context.execution_metadata``

    Type Preservation & Safety
    --------------------------
    - When a string is an exact single reference (e.g. ``"${steps.s1.row_count}"``),
      its native Python type (``int``, ``float``, ``bool``, ``list``, ``dict``,
      ``None``, ``str``) is preserved.
    - When a string embeds one or more references within larger text, string
      interpolation is performed.
    - Never uses ``eval()`` and never mutates the input parameter structure.
    """

    def resolve(
        self,
        parameters: dict[str, Any],
        context: ExecutionContext,
        *,
        step_id: str | None = None,
    ) -> dict[str, Any]:
        """Return a new dictionary with all dynamic references resolved against ``context``."""
        if not isinstance(parameters, dict):
            raise UnresolvedContextReferenceError(
                f"Step parameters must be a dictionary, got {type(parameters).__name__}.",
                workflow_id=context.workflow_id,
                workflow_version=context.workflow_version,
                execution_id=context.execution_id,
                step_id=step_id,
            )
        resolved = self.resolve_value(parameters, context, step_id=step_id)
        assert isinstance(resolved, dict)
        return resolved

    def resolve_value(
        self,
        value: Any,
        context: ExecutionContext,
        *,
        step_id: str | None = None,
    ) -> Any:
        """Recursively resolve ``value`` against ``context`` without mutating ``value``."""
        if isinstance(value, dict):
            return {k: self.resolve_value(v, context, step_id=step_id) for k, v in value.items()}
        if isinstance(value, list):
            return [self.resolve_value(item, context, step_id=step_id) for item in value]
        if isinstance(value, str):
            return self._resolve_string(value, context, step_id=step_id)
        return copy.deepcopy(value)

    def _resolve_string(
        self,
        raw: str,
        context: ExecutionContext,
        *,
        step_id: str | None = None,
    ) -> Any:
        if "${" not in raw:
            return raw

        tokens = self._parse_tokens(raw, context=context, step_id=step_id)
        # Single full-string reference preserves native Python type
        if len(tokens) == 1 and tokens[0][0] == "ref" and tokens[0][1] == raw:
            expr = tokens[0][2]
            return copy.deepcopy(
                self._evaluate_reference(
                    expr,
                    full_token=raw,
                    context=context,
                    step_id=step_id,
                )
            )

        # Interpolated string with literals and/or multiple references
        parts: list[str] = []
        for kind, token_text, expr in tokens:
            if kind == "literal":
                parts.append(token_text)
            else:
                resolved_val = self._evaluate_reference(
                    expr,
                    full_token=token_text,
                    context=context,
                    step_id=step_id,
                )
                parts.append(str(resolved_val))
        return "".join(parts)

    @staticmethod
    def _parse_tokens(
        raw: str,
        *,
        context: ExecutionContext,
        step_id: str | None = None,
    ) -> list[tuple[str, str, str]]:
        """Tokenize ``raw`` into ``('literal', text, '')`` and ``('ref', full_ref, expr)``."""
        tokens: list[tuple[str, str, str]] = []
        idx = 0
        length = len(raw)

        while idx < length:
            start = raw.find("${", idx)
            if start == -1:
                tokens.append(("literal", raw[idx:], ""))
                break

            if start > idx:
                tokens.append(("literal", raw[idx:start], ""))

            end = raw.find("}", start + 2)
            if end == -1:
                raise UnresolvedContextReferenceError(
                    f"Malformed context reference in '{raw}': missing closing '}}'.",
                    reference=raw[start:],
                    workflow_id=context.workflow_id,
                    workflow_version=context.workflow_version,
                    execution_id=context.execution_id,
                    step_id=step_id,
                )

            expr = raw[start + 2 : end]
            full_token = raw[start : end + 1]
            if not expr or not expr.strip() or "${" in expr or "{" in expr:
                raise UnresolvedContextReferenceError(
                    f"Malformed context reference '{full_token}'.",
                    reference=full_token,
                    workflow_id=context.workflow_id,
                    workflow_version=context.workflow_version,
                    execution_id=context.execution_id,
                    step_id=step_id,
                )

            tokens.append(("ref", full_token, expr.strip()))
            idx = end + 1

        return tokens

    def _evaluate_reference(
        self,
        expr: str,
        *,
        full_token: str,
        context: ExecutionContext,
        step_id: str | None = None,
    ) -> Any:
        segments = expr.split(".")
        if any(not seg or not seg.strip() for seg in segments):
            raise UnresolvedContextReferenceError(
                f"Malformed reference path in '{full_token}'.",
                reference=full_token,
                workflow_id=context.workflow_id,
                workflow_version=context.workflow_version,
                execution_id=context.execution_id,
                step_id=step_id,
            )

        namespace = segments[0]
        if namespace == "steps":
            if len(segments) < 3:
                raise UnresolvedContextReferenceError(
                    f"Step output reference '{full_token}' must have format "
                    "'${steps.<step_id>.<output_key>}'.",
                    reference=full_token,
                    workflow_id=context.workflow_id,
                    workflow_version=context.workflow_version,
                    execution_id=context.execution_id,
                    step_id=step_id,
                )
            ref_step_id = segments[1]
            if not context.has_step_output(ref_step_id):
                raise UnresolvedContextReferenceError(
                    f"Cannot resolve '{full_token}': step '{ref_step_id}' has no recorded outputs.",
                    reference=full_token,
                    workflow_id=context.workflow_id,
                    workflow_version=context.workflow_version,
                    execution_id=context.execution_id,
                    step_id=step_id,
                )
            step_output = context.get_step_output(ref_step_id)
            return self._traverse_path(
                step_output,
                segments[2:],
                full_token=full_token,
                context=context,
                step_id=step_id,
            )

        if namespace in ("variables", "workflow"):
            if len(segments) < 2:
                raise UnresolvedContextReferenceError(
                    f"Variable reference '{full_token}' must specify a variable name.",
                    reference=full_token,
                    workflow_id=context.workflow_id,
                    workflow_version=context.workflow_version,
                    execution_id=context.execution_id,
                    step_id=step_id,
                )
            var_name = segments[1]
            if not context.has_variable(var_name):
                raise UnresolvedContextReferenceError(
                    f"Cannot resolve '{full_token}': "
                    f"workflow variable '{var_name}' is not defined.",
                    reference=full_token,
                    workflow_id=context.workflow_id,
                    workflow_version=context.workflow_version,
                    execution_id=context.execution_id,
                    step_id=step_id,
                )
            var_val = context.get_variable(var_name)
            if len(segments) == 2:
                return var_val
            return self._traverse_path(
                var_val,
                segments[2:],
                full_token=full_token,
                context=context,
                step_id=step_id,
            )

        if namespace == "metadata":
            if len(segments) < 2:
                raise UnresolvedContextReferenceError(
                    f"Metadata reference '{full_token}' must specify a metadata key.",
                    reference=full_token,
                    workflow_id=context.workflow_id,
                    workflow_version=context.workflow_version,
                    execution_id=context.execution_id,
                    step_id=step_id,
                )
            meta_key = segments[1]
            if not context.has_metadata(meta_key):
                raise UnresolvedContextReferenceError(
                    f"Cannot resolve '{full_token}': metadata key '{meta_key}' is not defined.",
                    reference=full_token,
                    workflow_id=context.workflow_id,
                    workflow_version=context.workflow_version,
                    execution_id=context.execution_id,
                    step_id=step_id,
                )
            meta_val = context.get_metadata(meta_key)
            if len(segments) == 2:
                return meta_val
            return self._traverse_path(
                meta_val,
                segments[2:],
                full_token=full_token,
                context=context,
                step_id=step_id,
            )

        raise UnresolvedContextReferenceError(
            f"Unsupported reference namespace '{namespace}' in '{full_token}'. "
            "Supported namespaces: 'steps', 'variables', 'workflow', 'metadata'.",
            reference=full_token,
            workflow_id=context.workflow_id,
            workflow_version=context.workflow_version,
            execution_id=context.execution_id,
            step_id=step_id,
        )

    @staticmethod
    def _traverse_path(
        root: Any,
        keys: list[str],
        *,
        full_token: str,
        context: ExecutionContext,
        step_id: str | None = None,
    ) -> Any:
        current = root
        for key in keys:
            if not isinstance(current, dict):
                raise UnresolvedContextReferenceError(
                    f"Cannot resolve key '{key}' in '{full_token}': "
                    f"intermediate value is {type(current).__name__}, not a dict.",
                    reference=full_token,
                    workflow_id=context.workflow_id,
                    workflow_version=context.workflow_version,
                    execution_id=context.execution_id,
                    step_id=step_id,
                )
            if key not in current:
                raise UnresolvedContextReferenceError(
                    f"Key '{key}' not found while resolving '{full_token}'.",
                    reference=full_token,
                    workflow_id=context.workflow_id,
                    workflow_version=context.workflow_version,
                    execution_id=context.execution_id,
                    step_id=step_id,
                )
            current = current[key]
        return current


__all__ = ["ContextResolver"]
