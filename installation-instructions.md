# Installation Instructions

## Using Conda (Recommended)

1. Create the folder structure and place all files in their respective locations:
   ```
   mkdir -p synapse-metadata-corrector/templates
   # Copy all Python files to synapse-metadata-corrector/
   # Copy all HTML files to synapse-metadata-corrector/templates/
   ```

2. Navigate to the project directory:
   ```
   cd synapse-metadata-corrector
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

## Project Structure

Ensure your project has this structure:
```
synapse-metadata-corrector/
├── app.py
├── environment.yml
├── README.md
├── uploads/          # Will be created automatically
└── templates/
    ├── base.html
    ├── index.html
    ├── corrections.html
    └── download.html
```

## Troubleshooting

### Python/Pandas Compatibility Issues

The error you encountered is because pandas 2.1.0 isn't compatible with Python 3.13. In the environment.yml file, we're using:
- Python 3.10 (stable and widely supported)
- pandas 1.3.0 or newer, but less than 2.0.0 (compatible with our Python version)

If you need to use a specific Python version for other projects, consider creating a dedicated conda environment for this application.

### Manual Installation Alternative

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
