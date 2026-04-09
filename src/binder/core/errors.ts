export class BindError extends Error {
  override readonly name = 'BindError';

  constructor(message: string) {
    super(`Bind error: ${message}`);
  }
}
