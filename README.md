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

### Using Conda (Recommended)

1. Clone this repository.


2. Navigate to the project directory:
   ```
   cd metadata-prot
   ```

3. Create the Conda environment:
   ```
   conda env create -f environment.yml
   ```

4. Activate the environment:
   ```
   conda activate synapse-corrector
   ```

5. Run the application:
   ```
   python app.py
   ```

6. Open your browser and go to http://localhost:5000

### Alternative Installation (Manual)

If you prefer not to use the environment.yml file:

1. Create a new environment with a compatible Python version:
   ```
   conda create -n synapse-corrector python=3.10
   conda activate synapse-corrector
   ```

2. Install the core dependencies:
   ```
   conda install flask pandas=1.5
   ```

3. Install remaining dependencies:
   ```
   pip install Werkzeug Jinja2 MarkupSafe itsdangerous
   ```

## Project Structure

```
metadata-prot/
├── app.py                   # Main Flask application
├── environment.yml          # Conda environment specification
├── requirements.txt         # Python dependencies (alternative to environment.yml)
├── uploads/                 # Temporary storage for uploaded files
├── templates/               # HTML templates
│   ├── base.html            # Base template with common elements
│   ├── index.html           # File upload page
│   ├── analysis.html        # Analysis visualization page
│   ├── corrections.html     # Corrections review page
│   └── download.html        # Download page
└── README.md                # This file
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

## Troubleshooting

### Python/Pandas Compatibility Issues

The error you may encounter is because pandas 2.1.0 isn't compatible with Python 3.13. In the environment.yml file, we're using:
- Python 3.10 (stable and widely supported)
- pandas 1.3.0 or newer, but less than 2.0.0 (compatible with our Python version)

If you need to use a specific Python version for other projects, consider creating a dedicated conda environment for this application.

## Requirements

- Python 3.10+
- Flask
- pandas (1.3.0 - 1.5.x)
- numpy
- difflib (standard library)

## Limitations

- Large files (>100MB) may be slow to process in the browser
- Complex validation rules beyond simple value matching are not supported
- The application does not validate relationship constraints or cross-field validations

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
