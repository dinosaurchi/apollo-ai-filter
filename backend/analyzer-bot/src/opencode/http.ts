export function serverBaseUrl(serverUrl: string): string {
  return serverUrl.replace(/\/+$/, "");
}

function authHeader(env: NodeJS.ProcessEnv): string | null {
  const password = env.OPENCODE_SERVER_PASSWORD;
  if (!password) return null;
  const username = env.OPENCODE_SERVER_USERNAME ?? "opencode";
  return `Basic ${Buffer.from(`${username}:${password}`).toString("base64")}`;
}

export async function requestJson<T>(
  url: string,
  init: RequestInit,
  env: NodeJS.ProcessEnv,
): Promise<T> {
  const headers = new Headers(init.headers ?? {});
  headers.set("Accept", "application/json");
  if (init.body !== undefined) headers.set("Content-Type", "application/json");

  const auth = authHeader(env);
  if (auth) headers.set("Authorization", auth);

  let response: Response;
  try {
    response = await fetch(url, { ...init, headers });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Request failed for ${url}: ${message}`);
  }

  const raw = await response.text();
  if (!response.ok) {
    const reason = raw.trim().length > 0 ? raw.trim() : response.statusText;
    throw new Error(`HTTP ${response.status} ${response.statusText}: ${reason}`);
  }

  if (raw.length === 0) {
    return {} as T;
  }

  try {
    return JSON.parse(raw) as T;
  } catch {
    throw new Error(`Expected JSON response from ${url}, got: ${raw.slice(0, 500)}`);
  }
}
