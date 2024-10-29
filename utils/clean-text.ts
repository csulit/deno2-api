export function cleanText(input: string): string {
  if (!input) return "No description";

  // Encode special characters to ensure they are properly interpreted by the SQL engine
  const encodedString = encodeURIComponent(input);

  // Remove emojis and other special characters
  const cleanedString = encodedString.replace(
    /[\u{1F600}-\u{1F64F}\u{1F300}-\u{1F5FF}\u{1F680}-\u{1F6FF}\u{1F1E0}-\u{1F1FF}\u{2600}-\u{26FF}\u{2700}-\u{27BF}]/gu,
    "",
  );

  // Remove extra whitespace
  const trimmedString = cleanedString.replace(/\s+/g, " ").trim();

  // Remove any remaining non-printable characters
  return trimmedString.replace(/[^\x20-\x7E]/g, "");
}
