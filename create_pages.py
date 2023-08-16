import csv
from jinja2 import Environment, FileSystemLoader

# Start Python server in Terminal (this directory) with command python -m http.server
# Go to http://localhost:8000/search.html in browser

# Set up Jinja2 environment
env = Environment(loader=FileSystemLoader(''))


def generate_static_pages():
    with open('shipments.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            bl_number = row['BL']

            # Use the same 'result.html' template to generate a page for each BL number
            template = env.get_template('result.html')
            rendered_page = template.render(shipment=row)

            # Save the rendered page to a static HTML file named after the BL number
            with open(f'{bl_number}.html', 'w') as output_file:
                output_file.write(rendered_page)


# Call the function to generate static pages
# generate_static_pages()



