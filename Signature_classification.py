import asyncio
import aiohttp
import pandas as pd
from tqdm import tqdm
import re
import time

# --------------------------
# API Configuration
# --------------------------
API_KEY = "Psl4cZtS4kqlGlr8UubO5Wdm0CuRCRGd"
ENDPOINT = "https://eyq-incubator.asiapac.fabric.ey.com/eyq/as/api/openai/deployments/gpt-4-turbo/chat/completions"
API_VERSION = "2023-05-15"

# --------------------------
# Load Known Data (for reference)
# --------------------------
known_path = "C:/Users/AX126XF/OneDrive - EY/Documents/Normalisation_tool/known_data.csv"
known_df = pd.read_csv(known_path).dropna(subset=["Signature"])

# Normalize known signatures for better matching
known_df["Signature"] = known_df["Signature"].str.strip().str.lower()

# --------------------------
# Prompt Creation
# --------------------------
def create_prompt(software_name, reference_df):
    normalized_input = software_name.strip().lower()
    similar_entries = reference_df[reference_df['Signature'].str.contains(re.escape(normalized_input), na=False)]

    example_text = ""
    for _, row in similar_entries.iterrows():
        example_text += f"""Example:
1. Original Product Name: {row['Original Product Name']}
2. Publisher: {row['Publisher']}
3. Normalized Product Name: {row['Normalized Product Name']}
4. Product Edition: {row['Product Edition']}
5. Product Version: {row['Product Version']}
6. Free/Paid: {row['Licensable/Free']}
7. Desktop/Non-Desktop: {row['Desktop/Non-Desktop']}\n"""

    prompt = f"""You are a software licensing analyst. Based on the software name provided and known examples, generate a normalized SAM (Software Asset Management) profile for the given input.

{example_text if example_text else "No known examples available."}

Now analyze this software: "{software_name}"

Respond ONLY with the following format:
1. Original Product Name: <value>
2. Publisher: <value>(Vendor offering the software product)
3. Normalized Product Name: <value>
4. Product Edition: <value>
5. Product Version: <value>
6. Free/Paid: <value>
7. Desktop/Non-Desktop: Yes/No

No extra text. No explanations."""
    return prompt

# --------------------------
# Parse the AI Response
# --------------------------
def parse_response(response_text):
    keys = [
        "Original Product Name", "Publisher", "Normalized Product Name",
        "Product Edition", "Product Version", "Free/Paid", "Desktop/Non-Desktop"
    ]
    data = {key: "n/p" for key in keys}
    pattern = re.compile(r"^\d+\.\s*(.*?):\s*(.*)$")

    for line in response_text.strip().splitlines():
        match = pattern.match(line.strip())
        if match:
            key = match.group(1).strip()
            value = match.group(2).strip()
            for expected in keys:
                if key.lower() == expected.lower():
                    data[expected] = value
    return data

# --------------------------
# Call the API with Retry
# --------------------------
async def fetch_categorization(session, software_name):
    prompt = create_prompt(software_name, known_df)
    headers = {"api-key": API_KEY}
    params = {"api-version": API_VERSION}
    body = {"messages": [{"role": "user", "content": prompt}]}

    for attempt in range(2):
        try:
            async with session.post(ENDPOINT, headers=headers, params=params, json=body) as response:
                if response.status == 200:
                    result = await response.json()
                    content = result["choices"][0]["message"]["content"]
                    parsed = parse_response(content)
                    if list(parsed.values()).count("n/p") > 5:
                        continue  # Retry if response is mostly invalid
                    return parsed
                else:
                    print(f"⚠️ API error {response.status} for '{software_name}'")
        except Exception as e:
            print(f"❌ Exception during API call for '{software_name}': {e}")
        await asyncio.sleep(1.5)  # Brief pause before retry

    return {key: "n/p" for key in [
        "Original Product Name", "Publisher", "Normalized Product Name",
        "Product Edition", "Product Version", "Free/Paid", "Desktop/Non-Desktop"
    ]}

# --------------------------
# Async Main Function
# --------------------------
async def main(input_path, output_path):
    df = pd.read_csv(input_path)
    software_names = df.iloc[:, 0].dropna().tolist()

    results = []
    seen = {}

    async with aiohttp.ClientSession() as session:
        for name in tqdm(software_names, desc="Processing"):
            name_cleaned = name.strip()
            if name_cleaned in seen:
                result = seen[name_cleaned].copy()
            else:
                result = await fetch_categorization(session, name_cleaned)
                seen[name_cleaned] = result
            result["Input Signature"] = name_cleaned
            results.append(result)

    output_df = pd.DataFrame(results)

    # Desired column order
    column_order = [
        "Input Signature", "Original Product Name", "Publisher", "Normalized Product Name",
        "Product Edition", "Product Version", "Free/Paid", "Desktop/Non-Desktop"
    ]
    output_df = output_df[column_order]
    output_df.to_csv(output_path, index=False)
    print(f"✅ Output saved to: {output_path}")

# --------------------------
# Entry Point
# --------------------------
if __name__ == "__main__":
    input_path = "C:/Users/AX126XF/OneDrive - EY/Documents/Normalisation_tool/Signatures.csv"
    output_path = "C:/Users/AX126XF/OneDrive - EY/Documents/Normalisation_tool/Signatures_output.csv"
    asyncio.run(main(input_path, output_path))
