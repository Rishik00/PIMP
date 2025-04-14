import asyncio
import pandas as pd
from ollama import AsyncClient

SYSTEM_PROMPT = """You generate typo-squatted and lexically similar variations of given URLs. For each input URL, 
generate exactly two realistic and creative variations using one or more of the following techniques in combination 
to make them convincing:  

- Typos & Misspellings: Introduce common typing mistakes such as letter swaps, omissions, or duplications.  
- Subdomains & Prefixes: Add misleading subdomains or prefixes that mimic real ones.  
- Path & Directory Tricks: Slightly alter the URL path while keeping it believable.  
- Homograph Attacks: Use visually similar characters (e.g., replace 'o' with '0' or 'l' with '1').  
- Parameter Manipulation: Modify query parameters to create deceptive variations.  
- TLD & Domain Substitutions: Change top-level domains like '.com' to '.net' or introduce subtle domain misspellings.  
- Reordering Components: Rearrange parts of the URL to appear legitimate but altered.  

Combine multiple techniques in each variation to make them more deceptive and realistic. Avoid minor single-letter changes unless part of a larger modification.  

Output Format:  
original: <original_url>  
variation1: <synthetic_variation_1>  
variation2: <synthetic_variation_2>  

If no valid variations can be generated:  
original: <original_url>  
skipped: No valid variations generated.  

Only return the structured output without additional text or explanations."""  

async def inference(model_name: str, batch: list, output_file: str):
    """Generates typo-squatted variations using the LLM."""
    ollama_client = AsyncClient()
    prompt = f"Input URLs:\n{chr(10).join(batch)}"

    response = await ollama_client.chat(
        model=model_name,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT}, 
            {"role": "user", "content": prompt}
        ],
        ## Play with this parameter
        options={"temperature": 0.7}
    )

    # Extract response text and ensure structured formatting
    generated_text = response["message"]["content"].strip()
    
    # Write output to file
    with open(output_file, 'a') as f:
        f.write(generated_text + "\n\n")  # Needa a bit more checking

async def gendata(input_file: str, output_file: str, model_name: str, col='URL', batch_size: int=1, limit=0):
    """Reads URLs from a CSV, processes in batches, and saves output."""
    df = pd.read_csv(input_file)
    size = min(len(df), limit) if limit > 0 else len(df)

    for i in range(0, size, batch_size):
        batch = df[col].iloc[i:i+batch_size].tolist()
        await inference(model_name, batch, output_file)

    print("Processing complete. Output saved to", output_file)

# if __name__ == "__main__":
    ## Run this if you are in local
    # asyncio.run(gendata(input_file="random_malignant_5000.csv", output_file="output.txt", model_name="llama3.1:8b", limit=50))

    ## run this for colab, if you run the above one you get an error saying: already event loop running
    # await gendata(input_file="random_malignant_5000.csv", output_file="output.txt", model_name="llama3.1:8b", limit=50)