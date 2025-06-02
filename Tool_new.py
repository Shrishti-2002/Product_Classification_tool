import asyncio
import aiohttp
import pandas as pd
from tqdm import tqdm
import re

# --------------------------
# API Configuration
# --------------------------
API_KEY = "Psl4cZtS4kqlGlr8UubO5Wdm0CuRCRGd"
ENDPOINT = "https://eyq-incubator.asiapac.fabric.ey.com/eyq/as/api/openai/deployments/gpt-4-turbo/chat/completions"
API_VERSION = "2023-05-15"

# --------------------------
# Prompt Creation
# --------------------------
def create_prompt(software_name):
    return f"""
        #         As an expert in software categorization, please assist with the following task.
        #         Provide the following SAM analysis details for the {software_name} software product. Include columns for:
        #         1. Original Product Name(It is same as the input software name)
        #         2. Publisher(Vendor offering the software product)
        #         3. Normalized Product Name (cleaned for better readability)
        #         4. Product Edition (if applicable, otherwise mark as "n/p")
        #         5. Product Version (if applicable, otherwise mark as "n/p")
        #         6. Free/Paid (based on licensing model)
        #         7. Desktop/Non-Desktop(if product is installed on desktop or not)
        #
        #         **Format your response exactly as follows (do not include any extra text or greetings):**
        #
        #         1. Original Product Name
        #         2. Publisher
        #         3. Normalized Product Name
        #         4. Product Edition
        #         5. Product Version
        #         6. Free/Paid
        #         7. Desktop/Non-Desktop
        
        #     """


# --------------------------
# Response Parsing
# --------------------------
def parse_response(response_text):
    expected_keys = [
        "Original Product Name", "Publisher", "Normalized Product Name", "Product Edition",
        "Product Version", "Free/Paid", "Desktop/Non-Desktop"
    ]
    data = {key: "n/p" for key in expected_keys}
    pattern = re.compile(r"^\d+\.\s*(.*?)(?::|\s{2,})(.*)$")

    for line in response_text.strip().split("\n"):
        match = pattern.match(line)
        if match:
            key = match.group(1).strip()
            value = match.group(2).strip()
            for expected_key in expected_keys:
                if key.lower() == expected_key.lower():
                    data[expected_key] = value
                    break
    return data

# --------------------------
# API Call Logic
# --------------------------
async def fetch_categorization(session, software_name):
    prompt = create_prompt(software_name)
    headers = {"api-key": API_KEY}
    params = {"api-version": API_VERSION}
    body = {
        "messages": [{"role": "user", "content": prompt}]
    }

    async with session.post(ENDPOINT, headers=headers, params=params, json=body) as response:
        if response.status == 200:
            result = await response.json()
            content = result["choices"][0]["message"]["content"]
            parsed = parse_response(content) # Include input name
            return parsed
        else:
            print(f"Error: {response.status} for {software_name}")
            return {
                "Status": f"Error: {response.status}"
            }

# --------------------------
# Main Async Function
# --------------------------
async def main(input_file, output_file):
    df = pd.read_excel(input_file)
    software_names = df.iloc[:, 0].dropna().tolist()

    output_data = []
    async with aiohttp.ClientSession() as session:
        for name in tqdm(software_names, desc="Processing"):
            result = await fetch_categorization(session, name)
            output_data.append(result)

    output_df = pd.DataFrame(output_data)

    # Reorder columns
    column_order = [
        "Original Product Name",
        "Publisher",
        "Normalized Product Name",
        "Product Edition",
        "Product Version",
        "Free/Paid",
        "Desktop/Non-Desktop"
    ]
    for col in column_order:
        if col not in output_df.columns:
            output_df[col] = "n/p"
    output_df = output_df[column_order]

    output_df.to_excel(output_file, index=False)
    print(f"\n✅ Categorization completed. Output saved to: {output_file}")

# --------------------------
# Entry Point
# --------------------------
if __name__ == "__main__":
    input_path = "C:/Users/AX126XF/OneDrive - EY/Documents/Normalisation_tool/Signatures.csv"
    output_path = ("C:/Users/AX126XF/OneDrive - EY/Documents/Normalisation_tool/Signatures_output.csv")
    asyncio.run(main(input_path, output_path))
