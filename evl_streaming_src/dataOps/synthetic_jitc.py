#!/usr/bin/env python

"""
Application:        JITC processing
File name:          synthetic_jitc.py
Author:             Martin Manuel Lopez
Creation:           10/07/2024

The University of Arizona
Department of Electrical and Computer Engineering
College of Engineering
"""

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os
import random
import json
from faker import Faker  # Install using: pip install faker

def generate_html_files(num_files, output_dir):
    """
    Generates HTML files with varying content and proper English words.
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
            if content_choice == 'paragraphs':
                num_paragraphs = random.randint(2, 20)
                for _ in range(num_paragraphs):
                    paragraph = fake.paragraph(nb_sentences=20)
                    f.write(f"<p>{paragraph}</p>\n")
            elif content_choice == 'tables':
                num_rows = random.randint(2, 20)
                num_cols = random.randint(2, 20)
                f.write("<table border='1'>\n")
                for _ in range(num_rows):
                    f.write("<tr>\n")
                    for _ in range(num_cols):
                        cell_text = fake.sentence()
                        f.write(f"<td>{cell_text}</td>\n")
                    f.write("</tr>\n")
                f.write("</table>\n")
            elif content_choice == 'lists':
                num_items = random.randint(3, 10)
                f.write("<ul>\n")
                for _ in range(num_items):
                    item_text = fake.sentence()
                    f.write(f"<li>{item_text}</li>\n")
                f.write("</ul>\n")
            elif content_choice == 'headers':
                num_headers = random.randint(2, 20)
                for _ in range(num_headers):
                    header_level = random.randint(2, 6)
                    header_text = fake.sentence()
                    f.write(f"<h{header_level}>{header_text}</h{header_level}>\n")
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
