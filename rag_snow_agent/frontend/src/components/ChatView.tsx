import { useEffect, useRef } from "react";
import type { Message } from "../types";
import { ChatInput } from "./ChatInput";
import { LoadingIndicator } from "./LoadingIndicator";
import { MessageBubble } from "./MessageBubble";

interface Props {
  messages: Message[];
  isLoading: boolean;
  streamStatus: string;
  onSend: (text: string) => void;
  onCancel: () => void;
}

export function ChatView({ messages, isLoading, streamStatus, onSend, onCancel }: Props) {
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, streamStatus]);

  return (
    <div className="flex flex-col h-full">
      <div className="flex-1 overflow-y-auto px-4 py-6">
        {messages.length === 0 ? (
          <div className="flex items-center justify-center h-full text-gray-400">
            <div className="text-center">
              <div className="text-4xl mb-4">💬</div>
              <div className="text-lg font-medium">Analytics Insite</div>
              <div className="text-sm mt-1">
                Ask a question about your data, or any general question
              </div>
            </div>
          </div>
        ) : (
          <div className="max-w-4xl mx-auto space-y-4">
            {messages.map((msg) => (
              <MessageBubble key={msg.id} message={msg} />
            ))}
          </div>
        )}
        {isLoading && (
          <div className="max-w-4xl mx-auto">
            <LoadingIndicator status={streamStatus} />
            <div className="flex justify-center mt-2">
              <button
                onClick={onCancel}
                className="px-4 py-1.5 text-xs bg-red-100 text-red-700 rounded-lg hover:bg-red-200 transition-colors"
              >
                Stop generating
              </button>
            </div>
          </div>
        )}
        <div ref={bottomRef} />
      </div>
      <ChatInput onSend={onSend} disabled={isLoading} />
    </div>
  );
}
