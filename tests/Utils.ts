import type { Reader } from "@rdfc/js-runner";

export async function consumeOutput(outputStream: Reader, checkOutput: (query: string) => Promise<void>) {
    for await (const query of outputStream.strings()) {
        await checkOutput(query);
    }
}