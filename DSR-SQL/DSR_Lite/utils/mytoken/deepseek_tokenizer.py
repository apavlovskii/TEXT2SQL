import os
import glob

os.environ["TOKENIZERS_PARALLELISM"] = "false"
os.environ["TRANSFORMERS_VERBOSITY"] = "error"

try:
    from transformers import AutoTokenizer, logging  # type: ignore

    logging.set_verbosity_error()
    _TRANSFORMERS_AVAILABLE = True
except ModuleNotFoundError:
    AutoTokenizer = None  # type: ignore
    logging = None  # type: ignore
    _TRANSFORMERS_AVAILABLE = False

def truncate_text_by_tokens(text, max_tokens=4096):
    """
    Truncates the text so that its token count does not exceed max_tokens, and returns the truncated string.
    Automatically uses the tokenizer from the current script's directory.

    Args:
        text (str): The original string.
        max_tokens (int): The maximum number of tokens to truncate to.

    Returns:
        str: The truncated string.
    """
    if not _TRANSFORMERS_AVAILABLE:
        # Conservative fallback: approximate 4 chars/token.
        # This is only used when transformers isn't installed.
        approx_chars = max_tokens * 4
        return text[:approx_chars]

    current_dir = os.path.dirname(os.path.abspath(__file__))
    chat_tokenizer_dir = current_dir

    tokenizer = AutoTokenizer.from_pretrained(chat_tokenizer_dir, trust_remote_code=False)
    inputs = tokenizer(
        text,
        truncation=True,
        max_length=max_tokens,
        return_tensors=None,
        return_attention_mask=False,
        return_token_type_ids=False,
    )
    return tokenizer.decode(inputs["input_ids"], skip_special_tokens=True)


def get_token_count(text: str) -> int:
    """
    Calculates the number of tokens in a text.
    Automatically uses the tokenizer from the current script's directory.

    Args:
        text (str): The original string for which to calculate the token count.

    Returns:
        int: The number of tokens corresponding to the text.
    """
    if not _TRANSFORMERS_AVAILABLE:
        # Conservative fallback: approximate 4 chars/token.
        return max(1, len(text) // 4)

    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        current_dir = os.getcwd()

    tokenizer_dir = current_dir
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_dir, trust_remote_code=False)
    token_ids = tokenizer.encode(text)
    return len(token_ids)


SL='''
We are given a problem and a previous SQL query that was executed. We need to analyze the SQL based on the given steps and the reference columns.
'''


def analyze_all_markdown_files(directory):
    """
    Iterates through all .md files in the specified directory, reads their content,
    and outputs the token count for each file.

    Args:
        directory (str): The path to the directory containing the .md files.
    """
    # Use glob to find all .md files
    md_files = glob.glob(os.path.join(directory, "*.md"))
    
    if not md_files:
        print(f"No .md files found in the directory: {directory}")
        return

    print(f"Found {len(md_files)} Markdown files, calculating token counts...\n")
    
    for file_path in sorted(md_files):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            token_count = get_token_count(content)
            print(f"{os.path.basename(file_path)} | Tokens: {token_count}")
        except Exception as e:
            print(f"{os.path.basename(file_path)} | Failed to read: {e}")

# Main program entry point
if __name__ == "__main__":

    # Analyze all .md files in the entire directory
    doc_dir = "/spider2-snow/resource/documents"
    analyze_all_markdown_files(doc_dir)