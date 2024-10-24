import os
import random
import json
from faker import Faker  # Install using: pip install faker

def generate_html_files(num_files, output_dir):
    """
    Generates HTML files with varying content and proper English words,
    including randomized repeating sequences.
    """
    fake = Faker()
    content_blocks = []  # List to store content blocks

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for i in range(1, num_files + 1):
        filename = os.path.join(output_dir, f"file_{i}.html")
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("<html>\n<head>\n<title>Sample HTML File</title>\n</head>\n<body>\n")

            num_blocks = random.randint(5, 10)
            for _ in range(num_blocks):
                # Decide whether to create new content or repeat existing
                if content_blocks and random.random() < 0.5:
                    # Reuse existing content block
                    content_block = random.choice(content_blocks)
                else:
                    # Create new content block
                    content_types = ['paragraphs', 'tables', 'lists', 'headers']
                    content_choice = random.choice(content_types)
                    if content_choice == 'paragraphs':
                        num_paragraphs = random.randint(1, 3)
                        paragraphs = []
                        for _ in range(num_paragraphs):
                            paragraph = fake.paragraph(nb_sentences=5)
                            paragraphs.append(f"<p>{paragraph}</p>\n")
                        content_block = ''.join(paragraphs)
                    elif content_choice == 'tables':
                        num_rows = random.randint(2, 5)
                        num_cols = random.randint(2, 5)
                        table = "<table border='1'>\n"
                        for _ in range(num_rows):
                            table += "<tr>\n"
                            for _ in range(num_cols):
                                cell_text = fake.sentence()
                                table += f"<td>{cell_text}</td>\n"
                            table += "</tr>\n"
                        table += "</table>\n"
                        content_block = table
                    elif content_choice == 'lists':
                        num_items = random.randint(3, 7)
                        ul = "<ul>\n"
                        for _ in range(num_items):
                            item_text = fake.sentence()
                            ul += f"<li>{item_text}</li>\n"
                        ul += "</ul>\n"
                        content_block = ul
                    elif content_choice == 'headers':
                        num_headers = random.randint(1, 3)
                        headers = []
                        for _ in range(num_headers):
                            header_level = random.randint(1, 6)
                            header_text = fake.sentence()
                            headers.append(f"<h{header_level}>{header_text}</h{header_level}>\n")
                        content_block = ''.join(headers)
                    # Add new content block to the list
                    content_blocks.append(content_block)
                # Write content block to the file
                f.write(content_block)
            f.write("</body>\n</html>")

def convert_html_files_to_json_bits(input_dir, output_dir):
    """
    Converts HTML files into JSON files containing arrays of 0s and 1s.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    for filename in os.listdir(input_dir):
        if filename.endswith(".html"):
            input_path = os.path.join(input_dir, filename)
            output_filename = filename.replace('.html', '.json')
            output_path = os.path.join(output_dir, output_filename)
            with open(input_path, 'rb') as f_in, open(output_path, 'w') as f_out:
                bits_list = []
                byte = f_in.read(1)
                while byte:
                    bits = bin(ord(byte))[2:].zfill(8)
                    bits_list.extend([int(bit) for bit in bits])
                    byte = f_in.read(1)
                json.dump(bits_list, f_out)

# Usage
if __name__ == "__main__":
    num_files = 5000
    html_output_dir = "data/synthetic_jitc/html_files"
    json_output_dir = "data/synthetic_jitc/original_json_files"

    print("Generating HTML files...")
    generate_html_files(num_files, html_output_dir)

    print("Converting HTML files to JSON bit arrays...")
    convert_html_files_to_json_bits(html_output_dir, json_output_dir)

    print("Done!")
