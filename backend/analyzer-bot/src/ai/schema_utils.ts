import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import Ajv, { type ErrorObject } from "ajv";

type AjvLikeError = ErrorObject & {
  instancePath?: string;
  dataPath?: string;
  params: ErrorObject["params"];
};

function toPointerPath(path: string): string {
  if (path.length === 0) return "/";
  if (path.startsWith(".")) {
    return `/${path.slice(1).replace(/\[(\d+)\]/g, "/$1").replace(/\./g, "/")}`;
  }
  return path;
}

export function formatSchemaError(error: ErrorObject): string {
  const normalizedError = error as AjvLikeError;
  const rawPath =
    typeof normalizedError.instancePath === "string" && normalizedError.instancePath.length > 0
      ? normalizedError.instancePath
      : typeof normalizedError.dataPath === "string" && normalizedError.dataPath.length > 0
        ? normalizedError.dataPath
        : "/";
  const basePath = toPointerPath(rawPath);

  if (normalizedError.keyword === "required") {
    const missingProperty = (normalizedError.params as { missingProperty?: string }).missingProperty;
    const path = missingProperty ? `${basePath}/${missingProperty}`.replace(/\/+/g, "/") : basePath;
    return `${path}: ${normalizedError.message ?? "is required"}`;
  }

  if (normalizedError.keyword === "additionalProperties") {
    const property = (normalizedError.params as { additionalProperty?: string }).additionalProperty;
    const path = property ? `${basePath}/${property}`.replace(/\/+/g, "/") : basePath;
    return `${path}: ${normalizedError.message ?? "must NOT have additional properties"}`;
  }

  return `${basePath}: ${normalizedError.message ?? "invalid value"}`;
}

export function createSchemaValidator<T>(schemaFile: string): (obj: unknown) => { ok: true; value: T } | { ok: false; errors: string[] } {
  const schemaPath = resolve(process.cwd(), schemaFile);
  const schema = JSON.parse(readFileSync(schemaPath, "utf8")) as Record<string, unknown>;
  const ajv = new Ajv({ allErrors: true });
  const validate = ajv.compile(schema);

  return (obj: unknown) => {
    const valid = validate(obj);
    if (valid) {
      return { ok: true, value: obj as T };
    }
    return {
      ok: false,
      errors: (validate.errors ?? []).map(formatSchemaError),
    };
  };
}
