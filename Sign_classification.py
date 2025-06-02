import asyncio
import aiohttp
import pandas as pd
from tqdm import tqdm
import re
import time

# --------------------------
# API Configuration
# --------------------------]
API_KEY = "Psl4cZtS4kqlGlr8UubO5Wdm0CuRCRGd"
ENDPOINT = "https://eyq-incubator.asiapac.fabric.ey.com/eyq/as/api/openai/deployments/gpt-4-turbo/chat/completions"
API_VERSION = "2023-05-15"

# --------------------------
# Load Known Data (for reference)
# --------------------------
known_path = "C:/Users/AX126XF/OneDrive - EY/Documents/Normalisation_tool/known_data.csv"
known_df = pd.read_csv(known_path).dropna(subset=["Signature"])
known_df["Signature"] = known_df["Signature"].str.strip().str.lower()

# Sample a few representative examples once
example_known = known_df.sample(n=3, random_state=42).to_dict("records")

# --------------------------
# Prompt Creation
# --------------------------
def create_prompt(software_name, example_known):
    example_text = ""
    for row in example_known:
        example_text += f"""Example data:
1. Original Product Name: {row['Original Product Name']}
2. Publisher: {row['Publisher']}
3. Normalized Product Name: {row['Normalized Product Name']}
4. Version: {row['Version']}
5. Edition: {row['Edition']}
6. Free/Paid: {row['Licensable/Free']}
7. Desktop/Non-Desktop: {row['Desktop/Non-Desktop']}
8. Category: {row['Category']}
9. Product License Category: {row['Product License Category']}
10. End of Life: {row['End of Life']}
11. End of Support: {row['End of Support']}
12. Reference URL: {row['Reference URL']}\n"""

    prompt = f"""You are a software licensing analyst. Based on the software name and examples, generate a normalized SAM profile.

{example_text}

Now analyze this software: \"{software_name}\"
Respond in this format:
1. Original Product Name: <value>
2. Publisher: <value>(Vendor offering the software product)
3. Normalized Product Name: <value>
4. Version: <value>
5. Edition: <value>
6. Free/Paid: <value>(Only return either \"Free\" or \"Paid\" unless absolutely necessary to return \"both\", \"Free/Paid\", or anything else.)
7. Desktop/Non-Desktop: Yes/No
8. Category(SAM-Focused): <value>
9. Product License Category: <value>
10. End of Life: <value>
11. End of Support: <value>
12. Reference URL: <value>
No extra text. No explanations.
Try to find complete details for the input"""

    return prompt

# --------------------------
# Parse the AI Response
# --------------------------
def parse_response(response_text):
    keys = [
        "Original Product Name", "Publisher", "Normalized Product Name",
        "Version", "Edition", "Free/Paid", "Desktop/Non-Desktop", "Category",
        "Product License Category","End of Life", "End of Support","Reference URL"
    ]
    data = {key: "n/p" for key in keys}

    pattern = re.compile(r"^(?:\d+\.)?\s*(.*?):\s*(.*)$")

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
async def fetch_categorization(session, software_name, example_known):
    prompt = create_prompt(software_name, example_known)
    headers = {"api-key": API_KEY}
    params = {"api-version": API_VERSION}
    body = {"messages": [{"role": "user", "content": prompt}]}

    for attempt in range(3):
        try:
            async with session.post(ENDPOINT, headers=headers, params=params, json=body) as response:
                if response.status == 200:
                    result = await response.json()
                    content = result["choices"][0]["message"]["content"]
                    parsed = parse_response(content)

                    if sum(1 for v in parsed.values() if v.lower() in {"n/p", "unknown"}) > 5:
                        continue
                    return parsed
                else:
                    print(f"⚠️ API error {response.status} for '{software_name}'")
        except Exception as e:
            print(f"❌ Exception during API call for '{software_name}': {e}")
        await asyncio.sleep(1.5)

    return {key: "n/p" for key in [
        "Original Product Name", "Publisher", "Normalized Product Name",
        "Version", "Edition", "Free/Paid", "Desktop/Non-Desktop", "Category",
        "Product License Category","End of Life", "End of Support","Reference URL"
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
            cache_key = name_cleaned.lower()
            if cache_key in seen:
                result = seen[cache_key].copy()
            else:
                result = await fetch_categorization(session, name_cleaned, example_known)
                seen[cache_key] = result

            result["Input Signature"] = name_cleaned
            results.append(result)

            if all(v.lower() in {"n/p", "unknown"} for v in result.values() if isinstance(v, str)):
                print(f"⚠️ Poor result for: {name_cleaned}")

    output_df = pd.DataFrame(results)

    column_order = [
        "Input Signature", "Original Product Name", "Publisher", "Normalized Product Name",
        "Version", "Edition", "Free/Paid", "Desktop/Non-Desktop", "Category",
        "Product License Category","End of Life", "End of Support","Reference URL"
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