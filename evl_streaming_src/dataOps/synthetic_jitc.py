import os
import random
import json
from faker import Faker  # Install using: pip install faker

def generate_html_files(num_files, output_dir):
    """
    Generates HTML files with varying content and proper English words.
    Ensures each file has more than 32 bytes worth of data.
    """
    fake = Faker()
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    for i in range(1, num_files + 1):
        filename = os.path.join(output_dir, f"file_{i}.html")
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("<html>\n<head>\n<title>Sample HTML File</title>\n</head>\n<body>\n")
            content_types = ['paragraphs', 'tables', 'lists', 'headers']
            content_choice = random.choice(content_types)
            content_size = 0
            
            # Keep adding content until file size exceeds 32 bytes
            while content_size <= 32:
                if content_choice == 'paragraphs':
                    # Generate one paragraph to repeat
                    paragraph = fake.paragraph(nb_sentences=20)
                    num_paragraphs = random.randint(1, 10)
                    for _ in range(num_paragraphs):
                        f.write(f"<p>{paragraph}</p>\n")  # Repeat same paragraph
                        content_size += len(paragraph)
                    # Add the repeated content at least once
                    f.write(f"<p>{paragraph}</p>\n")
                    content_size += len(paragraph)
                
                elif content_choice == 'tables':
                    num_rows = random.randint(1, 10)
                    num_cols = random.randint(1, 10)
                    f.write("<table border='1'>\n")
                    # Generate a row to repeat
                    repeated_row = [fake.sentence() for _ in range(num_cols)]
                    for _ in range(num_rows):
                        f.write("<tr>\n")
                        for cell_text in repeated_row:
                            f.write(f"<td>{cell_text}</td>\n")  # Repeat the same row
                            content_size += len(cell_text)
                        f.write("</tr>\n")
                    # Add the repeated row again at least once
                    f.write("<tr>\n")
                    for cell_text in repeated_row:
                        f.write(f"<td>{cell_text}</td>\n")  # Repeat row again
                        content_size += len(cell_text)
                    f.write("</tr>\n")
                    f.write("</table>\n")
                
                elif content_choice == 'lists':
                    # Generate one list item to repeat
                    repeated_item = fake.sentence()
                    num_items = random.randint(1, 10)
                    f.write("<ul>\n")
                    for _ in range(num_items):
                        f.write(f"<li>{repeated_item}</li>\n")  # Repeat the same list item
                        content_size += len(repeated_item)
                    # Add the repeated item at least once more
                    f.write(f"<li>{repeated_item}</li>\n")
                    content_size += len(repeated_item)
                    f.write("</ul>\n")
                
                elif content_choice == 'headers':
                    # Generate a header to repeat
                    repeated_header = fake.sentence()
                    header_level = random.randint(1, 5)
                    num_headers = random.randint(1, 5)
                    for _ in range(num_headers):
                        f.write(f"<h{header_level}>{repeated_header}</h{header_level}>\n")  # Repeat same header
                        content_size += len(repeated_header)
                    # Add the repeated header at least once more
                    f.write(f"<h{header_level}>{repeated_header}</h{header_level}>\n")
                    content_size += len(repeated_header)
            
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
