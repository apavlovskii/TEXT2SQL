interface Props {
  error: string;
}

export function ErrorCard({ error }: Props) {
  return (
    <div className="mt-2 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
      <span className="font-medium">Error:</span> {error}
    </div>
  );
}
