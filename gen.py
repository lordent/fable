import ast
import inspect

import sql.functions  # noqa
from sql.core import expressions


def get_annotation_as_str(node) -> str:
    if node is None:
        return "Any"

    try:
        return ast.unparse(node)
    except AttributeError:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Constant):
            return str(node.value)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            return f"{get_annotation_as_str(node.left)} | {get_annotation_as_str(node.right)}"
        return "Any"


def generate_auto_overloads(cls: type):
    source = inspect.getsource(cls)
    tree = ast.parse(source)

    internal_types = {cls.__name__}

    def deep(cls_: type):
        for sub in cls_.__subclasses__():
            deep(sub)
            internal_types.add(sub.__name__)

    deep(cls)

    class_def = next(
        n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == cls.__name__
    )

    overloads = []

    for node in class_def.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            ret_node = node.returns
            is_internal = False

            ret_name = None
            if isinstance(ret_node, ast.Name):
                ret_name = ret_node.id
            elif isinstance(ret_node, ast.Constant):
                ret_name = ret_node.value

            if ret_name in internal_types:
                is_internal = True

            if not is_internal:
                continue

            raw_args = node.args.args[1:]

            args_info = []
            for a in raw_args:
                anno = get_annotation_as_str(a.annotation) or "Any"
                args_info.append({"name": a.arg, "type": anno, "is_any": anno == "Any"})

            args_str = ", ".join([f"{a['name']}: {a['type']}" for a in args_info])
            overloads.append(
                f"    @overload\n    def {node.name}(self: AggregateExpression, {args_str}) -> AggregateExpression: ..."
            )

            for i, target_arg in enumerate(args_info):
                if target_arg["is_any"]:
                    current_args = []
                    for j, a in enumerate(args_info):
                        t = "AggregateExpression" if i == j else a["type"]
                        current_args.append(f"{a['name']}: {t}")

                    overloads.append(
                        f"    @overload\n    def {node.name}(self: Any, {', '.join(current_args)}) -> AggregateExpression: ..."
                    )

            overloads.append(
                f"    @overload\n    def {node.name}(self: Any, {args_str}) -> Expression: ..."
            )

    return "\n".join(overloads)


print(generate_auto_overloads(expressions.Expression))
