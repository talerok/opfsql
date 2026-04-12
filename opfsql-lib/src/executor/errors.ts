export class ExecutorError extends Error {
  override readonly name = 'ExecutorError';
  constructor(message: string) {
    super(`Executor error: ${message}`);
  }
}
