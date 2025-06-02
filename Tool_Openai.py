import asyncio
import aiohttp

# API Configuration
API_KEY = "Psl4cZtS4kqlGlr8UubO5Wdm0CuRCRGd"
ENDPOINT = "https://eyq-incubator.asiapac.fabric.ey.com/eyq/as/api/openai/deployments/gpt-4-turbo/chat/completions"
API_VERSION = "2023-05-15"

def create_prompt(software_name):
    prompt = f"""
    #         As an expert in software categorization, please assist with the following task.
    #         Provide the following SAM analysis details for the {software_name} software product. Include columns for:
    #         1. Original Product Name
    #         2. Publisher
    #         3. Normalized Product Name (cleaned for better readability)
    #         4. Product Edition (if applicable, otherwise mark as "n/p")
    #         5. Product Version (if applicable, otherwise mark as "n/p")
    #         6. Category (SAM-focused) (e.g., Development Framework, Database Management, Enterprise Software, etc.)
    #         7. Free/Paid (based on licensing model)
    #         8. Product License Category (e.g. Suite, Component, Individual purchase, Setup, etc.)
    #         9. End of Life (if available, otherwise mark as "n/p" or "n/a" if still active)
    #         10. End of Support (if available, otherwise mark as "n/p" or "n/a" if still active)
    #         11. Reference URL (official site for more details, use plain text URLs instead of hyperlinks)
    #
    #         **Format your response exactly as follows (do not include any extra text or greetings):**
    #
    #         1. Original Product Name
    #         2. Publisher
    #         3. Normalized Product Name
    #         4. Product Edition
    #         5. Product Version
    #         6. Category
    #         7. Free/Paid
    #         8. Product License Category
    #         9. End of Life
    #         10. End of Support
    #         11. Reference URL
    #     """
    return prompt

async def get_categorization(software_name, parent_company):
    prompt = create_prompt(software_name)

    headers = {
        "api-key": API_KEY
    }

    params = {
        "api-version": API_VERSION
    }

    body = {
        "messages": [{"role": "user", "content": prompt}]
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(ENDPOINT, headers=headers, params=params, json=body) as response:
            if response.status == 200:
                result = await response.json()
                print(result["choices"][0]["message"]["content"])
            else:
                print(f"Error: {response.status}")
                print(await response.text())

if __name__ == "__main__":
    software = input("Enter software name: ")
    company = input("Enter parent company: ")
    asyncio.run(get_categorization(software, company))
