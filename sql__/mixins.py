from .query import Q


class SelectValuesMixin:
    def values(self, *args, **kwargs):
        from .field import Field

        for value in args:
            if isinstance(value, Field):
                self.dependencies.update(value.dependencies)
                self._fields[value.name] = value
            else:
                raise ValueError(f"Expected Field, got {type(value)}")

        for name, value in kwargs.items():
            self._fields[name] = value
            self._update_deps(value)
        return self

    def _update_deps(self, value):
        if isinstance(value, Q):
            self.dependencies.update(value.dependencies)
        elif isinstance(value, dict):
            for v in value.values():
                self._update_deps(v)

    def _json_build_object_recursive(self, fields, args):
        tokens = []
        for name, value in fields.items():
            tokens.append(f"'{name}'")

            if isinstance(value, Q):
                tokens.append(value.compile(args))
            elif isinstance(value, dict):
                tokens.append(self._json_build_object_recursive(value, args))
            else:
                args.append(value)
                tokens.append(f"${len(args)}")

        return f"JSONB_BUILD_OBJECT({', '.join(tokens)})"
