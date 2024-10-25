import { OpenAI } from "npm:openai";

export const openaiAssistant = async (question: string) => {
  const openai = new OpenAI({
    apiKey: Deno.env.get("OPENAI_API_KEY"),
  });

  const assistant = await openai.beta.assistants.create({
    name: "Malcolm",
    instructions: `
      You are Malcolm, a professional real estate copywriter with over 15 years of experience. Your expertise lies in crafting compelling, sophisticated, and persuasive property descriptions that highlight the unique features and selling points of each property. When given an existing property description and key property details, you will create an enhanced, professional listing description that: 1) Uses engaging and elegant language 2) Emphasizes the most attractive features 3) Incorporates provided property specifications naturally 4) Maintains a professional tone while being descriptive 5) Follows real estate industry best practices for listing descriptions. Your goal is to help properties stand out in the market with descriptions that appeal to the target audience.

      You must return a valid JSON array of strings that can be parsed. Each string should be markdown compatible and not contain any special characters that would break JSON parsing.

      The response should follow this exact format:
      [
        "# Property Title",
        "## Property Description",
        "## Key Features",
        "- Feature 1",
        "- Feature 2",
        "## Additional Information", 
        "- Note 1",
        "- Note 2"
      ]

      Example of valid response:
      [
        "# Luxurious Downtown Penthouse",
        "## Description\\nElegant penthouse offering breathtaking city views and premium finishes throughout. This sophisticated residence combines modern design with timeless luxury.",
        "## Key Features",
        "- 3 Spacious Bedrooms",
        "- Gourmet Kitchen with Premium Appliances",
        "- Private Rooftop Terrace",
        "- Floor-to-Ceiling Windows",
        "## Additional Information",
        "- 24/7 Concierge Service",
        "- 2 Parking Spaces Included",
        "- Pet-Friendly Building"
      ]

      Important:
      1. Do not include any line breaks (\\n) except where explicitly shown
      2. Only use standard markdown syntax
      3. Ensure all quotes are properly escaped
      4. Only include location details from listing_address, listing_region_name, listing_city_name, and listing_area_name if provided
      5. The response must be a valid JSON array that can be parsed
      `,
    model: "gpt-4o",
  });

  const thread = await openai.beta.threads.create();

  await openai.beta.threads.messages.create(thread.id, {
    role: "user",
    content: question,
  });

  const run = await openai.beta.threads.runs.create(thread.id, {
    assistant_id: assistant.id,
  });

  let runStatus = await openai.beta.threads.runs.retrieve(thread.id, run.id);

  while (runStatus.status !== "completed") {
    await new Promise((resolve) => setTimeout(resolve, 2000));
    runStatus = await openai.beta.threads.runs.retrieve(thread.id, run.id);
  }

  const messages = await openai.beta.threads.messages.list(thread.id);

  const lastMessageForRun = messages.data
    .filter(
      (message) => message.run_id === run.id && message.role === "assistant"
    )
    .pop();

  if (lastMessageForRun) {
    if (lastMessageForRun.content[0].type === "text") {
      console.log(lastMessageForRun.content[0].text.value);
      await openai.beta.threads.del(thread.id);
      return lastMessageForRun.content[0].text.value;
    }
  }

  await openai.beta.threads.del(thread.id);

  return "";
};
