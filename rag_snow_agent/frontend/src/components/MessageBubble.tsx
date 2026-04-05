import type { Message } from "../types";
import { ErrorCard } from "./ErrorCard";
import { MetadataCard } from "./MetadataCard";
import { ResultTable } from "./ResultTable";
import { SqlCard } from "./SqlCard";

interface Props {
  message: Message;
}

export function MessageBubble({ message }: Props) {
  if (message.role === "user") {
    return (
      <div className="flex justify-end">
        <div className="max-w-2xl rounded-2xl rounded-br-md bg-blue-600 px-4 py-3 text-sm text-white whitespace-pre-wrap">
          {message.content}
        </div>
      </div>
    );
  }

  return (
    <div className="flex justify-start">
      <div className="max-w-3xl w-full">
        <div className="rounded-2xl rounded-bl-md bg-gray-100 px-4 py-3 text-sm text-gray-800 whitespace-pre-wrap">
          {message.content}
        </div>
        {message.error && <ErrorCard error={message.error} />}
        {message.results && <ResultTable results={message.results} />}
        {message.sql && <SqlCard sql={message.sql} />}
        {message.metadata && <MetadataCard metadata={message.metadata} />}
      </div>
    </div>
  );
}
