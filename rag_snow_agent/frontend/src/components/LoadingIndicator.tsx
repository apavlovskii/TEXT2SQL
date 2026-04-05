interface Props {
  status: string;
}

export function LoadingIndicator({ status }: Props) {
  return (
    <div className="flex items-center gap-3 px-4 py-3 max-w-4xl mx-auto">
      <div className="flex gap-1">
        <div className="w-2 h-2 bg-blue-500 rounded-full animate-bounce" style={{ animationDelay: "0ms" }} />
        <div className="w-2 h-2 bg-blue-500 rounded-full animate-bounce" style={{ animationDelay: "150ms" }} />
        <div className="w-2 h-2 bg-blue-500 rounded-full animate-bounce" style={{ animationDelay: "300ms" }} />
      </div>
      <span className="text-sm text-gray-500 italic">{status}</span>
    </div>
  );
}
