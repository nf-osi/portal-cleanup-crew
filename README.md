# Synapse Metadata Corrector

A Python web application for correcting metadata in CSV files exported from Synapse.org fileviews, using JSONLD schemas to identify and fix invalid values.

## Features

- Upload CSV files from Synapse fileviews and JSONLD schema files
- Automatically identify values that don't match the schema's valid options
- Generate intelligent suggestions for corrections based on similarity matching
- Provide multiple suggestions for each invalid value
- Interactive review interface to select which corrections to apply
- Export corrected CSV for upload back to Synapse

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/synapse-metadata-corrector.git
   cd synapse-metadata-corrector
   ```

2. Create a virtual environment (optional but recommended):
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

1. Start the application:
   ```
   python app.py
   ```

2. Open your browser and go to http://localhost:5000

3. Upload your CSV file from Synapse and the corresponding JSONLD schema

4. Review the suggested corrections

5. Apply selected corrections and download the corrected CSV

6. Upload the corrected CSV back to Synapse

## Project Structure

```
synapse-metadata-corrector/
├── app.py                   # Main Flask application
├── requirements.txt         # Python dependencies
├── uploads/                 # Temporary storage for uploaded files
├── templates/               # HTML templates
│   ├── base.html            # Base template with common elements
│   ├── index.html           # File upload page
│   ├── corrections.html     # Corrections review page
│   └── download.html        # Download page
└── README.md                # This file
```

## How It Works

1. **File Parsing**: The application parses CSV and JSONLD files using pandas and the JSON library
2. **Schema Analysis**: It extracts valid values from the JSONLD schema, looking for values in:
   - `schema:rangeIncludes` arrays
   - Classes with `rdfs:oneOf` enumerations
   - Properties with `sms:validationRules`
   - Display names from `sms:displayName` and `rdfs:label`
3. **Validation**: It checks each value in the CSV against the valid options in the schema
4. **Suggestion Generation**: For invalid values, it generates multiple suggestions using:
   - String similarity metrics
   - Word overlap analysis
   - Special handling for common data types
   - Preservation of array formats
5. **Correction Application**: Selected corrections are applied to the CSV data
6. **Export**: The corrected CSV is made available for download

## Requirements

- Python 3.7+
- Flask
- pandas
- numpy
- difflib (standard library)

## Limitations

- Large files (>100MB) may be slow to process in the browser
- Complex validation rules beyond simple value matching are not supported
- The application does not validate relationship constraints or cross-field validations

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.