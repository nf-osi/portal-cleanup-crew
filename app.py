import os
import json
import pandas as pd
import re
import difflib
import sys
from flask import Flask, render_template, request, jsonify, send_file, url_for
from werkzeug.utils import secure_filename

# Print version information for debugging
print(f"Python version: {sys.version}")
print(f"Pandas version: {pd.__version__}")

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max upload size

# Create uploads folder if it doesn't exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

class SynapseMetadataCorrector:
    def __init__(self):
        self.csv_data = None
        self.jsonld_data = None
        self.corrections = []
        
        # Synapse system metadata columns that should be ignored during validation
        self.synapse_system_columns = [
            'name', 'type', 'id', 'etag', 'createdOn', 'modifiedOn', 
            'createdBy', 'modifiedBy', 'parentId', 'currentVersion',
            'benefactorId', 'projectId', 'concreteType', 'versionNumber',
            'versionLabel', 'versionComment', 'dataFileHandleId', 'columnId',
            'versionNumber', 'ROW_ID', 'ROW_VERSION', 'ROW_ETAG', 'entityId',
            # Additional columns to skip
            'fundingAgency', 'accessType'
        ]
    
    def load_csv(self, file_path, row_limit=None):
        """Load CSV file into pandas DataFrame"""
        print(f"Loading CSV from {file_path}")
        
        try:
            if row_limit and int(row_limit) > 0:
                self.csv_data = pd.read_csv(file_path, nrows=int(row_limit))
            else:
                self.csv_data = pd.read_csv(file_path)
                
            print(f"Loaded CSV with {len(self.csv_data)} rows and {len(self.csv_data.columns)} columns")
            print(f"Columns: {list(self.csv_data.columns)}")
            return len(self.csv_data)
        except Exception as e:
            print(f"Error loading CSV: {str(e)}")
            raise
    
    def load_jsonld(self, file_path):
        """Load JSONLD schema"""
        print(f"Loading JSONLD from {file_path}")
        
        try:
            with open(file_path, 'r') as f:
                self.jsonld_data = json.load(f)
            
            if '@graph' in self.jsonld_data:
                print(f"JSONLD loaded with {len(self.jsonld_data['@graph'])} graph items")
            else:
                print("JSONLD loaded but no @graph found")
                
            return bool(self.jsonld_data)
        except Exception as e:
            print(f"Error loading JSONLD: {str(e)}")
            raise
    
    def extract_valid_values(self):
        """Extract valid values from JSONLD schema"""
        valid_values = {}
        
        if not self.jsonld_data or '@graph' not in self.jsonld_data:
            return valid_values
            
        # Create mapping of IDs to display names
        id_to_display_name = {}
        for item in self.jsonld_data['@graph']:
            if '@id' in item:
                display_name = None
                # Prioritize sms:displayName
                if 'sms:displayName' in item:
                    display_name = item['sms:displayName']
                elif 'rdfs:label' in item:
                    display_name = item['rdfs:label']
                elif 'schema:name' in item:
                    display_name = item['schema:name']
                else:
                    # Extract name from ID
                    display_name = item['@id'].split(':')[-1] if ':' in item['@id'] else item['@id']
                
                id_to_display_name[item['@id']] = display_name
        
        # Extract property values
        for item in self.jsonld_data['@graph']:
            property_name = None
            
            # Get property name with priority to display name
            if 'sms:displayName' in item:
                property_name = item['sms:displayName']
            elif 'rdfs:label' in item:
                property_name = item['rdfs:label']
            elif '@id' in item:
                property_name = item['@id'].split(':')[-1] if ':' in item['@id'] else item['@id']
            
            if not property_name:
                continue
                
            # Extract values for this property from different possible schema locations
            values = []
            
            # From schema:rangeIncludes
            if 'schema:rangeIncludes' in item and isinstance(item['schema:rangeIncludes'], list):
                for option in item['schema:rangeIncludes']:
                    if isinstance(option, dict) and '@id' in option:
                        ref_id = option['@id']
                        if ref_id in id_to_display_name:
                            values.append({
                                'id': ref_id,
                                'label': id_to_display_name[ref_id]
                            })
            
            # From rdfs:range and rdfs:oneOf
            if 'rdfs:range' in item and isinstance(item['rdfs:range'], dict) and '@id' in item['rdfs:range']:
                range_id = item['rdfs:range']['@id']
                # Find the referenced class
                for class_item in self.jsonld_data['@graph']:
                    if '@id' in class_item and class_item['@id'] == range_id and 'rdfs:oneOf' in class_item:
                        if isinstance(class_item['rdfs:oneOf'], list):
                            for option in class_item['rdfs:oneOf']:
                                if isinstance(option, dict) and '@id' in option:
                                    ref_id = option['@id']
                                    if ref_id in id_to_display_name:
                                        values.append({
                                            'id': ref_id,
                                            'label': id_to_display_name[ref_id]
                                        })
            
            # From validation rules
            if 'sms:validationRules' in item and isinstance(item['sms:validationRules'], list):
                for rule in item['sms:validationRules']:
                    if isinstance(rule, str):
                        values.append({
                            'id': rule,
                            'label': rule
                        })
            
            if values:
                valid_values[property_name] = values
        
        print(f"Extracted valid values for {len(valid_values)} properties")
        for prop, vals in list(valid_values.items())[:5]:  # Print first 5 for debugging
            print(f"  - {prop}: {len(vals)} values")
            
        return valid_values
    
    def find_best_matches(self, value, valid_options, max_matches=3):
        """Find the best matching suggestions for a value"""
        if not value or not valid_options:
            return []
            
        # Handle array values in JSON string format
        original_is_array = False
        original_array_format = None
        values_to_check = []
        
        if isinstance(value, str) and value.strip().startswith('[') and value.strip().endswith(']'):
            try:
                original_array_format = value
                parsed_array = json.loads(value)
                if isinstance(parsed_array, list):
                    values_to_check = parsed_array
                    original_is_array = True
                else:
                    values_to_check = [value]
            except json.JSONDecodeError:
                values_to_check = [value]
        elif isinstance(value, list):
            values_to_check = value
            original_is_array = True
        else:
            values_to_check = [value]
            
        # If empty array, return empty suggestions
        if not values_to_check:
            return []
            
        # Only process the first value in the array for suggestions
        first_value = values_to_check[0]
        normalized_value = str(first_value).lower().strip()
        
        # Calculate similarity scores
        matches = []
        for option in valid_options:
            option_label = option['label']
            option_id = option['id']
            
            # Calculate string similarity
            label_similarity = difflib.SequenceMatcher(None, normalized_value, 
                                                     str(option_label).lower().strip()).ratio()
            id_similarity = difflib.SequenceMatcher(None, normalized_value, 
                                                  str(option_id).lower().strip()).ratio()
            
            # Use the better of the two scores
            similarity = max(label_similarity, id_similarity)
            
            # Boost score for partial matches
            if normalized_value in str(option_label).lower():
                similarity += 0.2
            elif str(option_label).lower() in normalized_value:
                similarity += 0.1
                
            # Create a match entry
            matches.append({
                'id': option_id,
                'label': option_label,
                'score': similarity,
                'reason': f"Similarity: {similarity:.2f}"
            })
        
        # Sort by score
        matches.sort(key=lambda x: x['score'], reverse=True)
        
        # Get top matches
        top_matches = matches[:max_matches]
        
        # Format suggestions based on original format
        suggestions = []
        for match in top_matches:
            if original_is_array:
                if len(values_to_check) > 1:
                    # For multi-value arrays, replace just the first value
                    suggestion_values = [match['label']] + values_to_check[1:]
                else:
                    # For single-value arrays
                    suggestion_values = [match['label']]
                    
                if original_array_format:
                    # Return as JSON string
                    suggestions.append(json.dumps(suggestion_values))
                else:
                    # Return as Python list
                    suggestions.append(suggestion_values)
            else:
                # Return as simple string
                suggestions.append(match['label'])
                
        return suggestions
    
    def generate_corrections(self):
        """Generate correction suggestions for CSV data"""
        if self.csv_data is None or self.jsonld_data is None:
            return []
            
        # Extract valid values from schema
        valid_values = self.extract_valid_values()
        if not valid_values:
            return []
            
        # Create mapping between CSV columns and schema properties
        column_to_property_map = {}
        
        print(f"Processing {len(self.csv_data.columns)} columns for mapping")
        
        for column in self.csv_data.columns:
            # Skip Synapse system metadata columns
            if any(system_col.lower() == column.lower() for system_col in self.synapse_system_columns) or any(system_col.lower() in column.lower() for system_col in ['id', 'version']):
                print(f"Skipping system column: {column}")
                continue
                
            column_lower = column.lower().strip()
            
            # Try to find matching property
            matching_property = None
            for prop in valid_values.keys():
                if prop.lower().strip() == column_lower:
                    matching_property = prop
                    print(f"Exact match: {column} -> {prop}")
                    break
            
            # If no exact match, try partial match
            if not matching_property:
                for prop in valid_values.keys():
                    if prop.lower() in column_lower or column_lower in prop.lower():
                        matching_property = prop
                        print(f"Partial match: {column} -> {prop}")
                        break
            
            if matching_property:
                column_to_property_map[column] = matching_property
            else:
                print(f"No match found for column: {column}")
        
        print(f"Mapped {len(column_to_property_map)} columns to properties:")
        for col, prop in column_to_property_map.items():
            print(f"  - {col} -> {prop}")
        
        # Process data
        corrections = []
        
        # Process each row
        total_rows = len(self.csv_data)
        print(f"Processing {total_rows} rows...")
        
        # Debug: print the first few rows of data for tumorType
        if 'tumorType' in self.csv_data.columns:
            print("First 5 tumorType values:")
            for idx in range(min(5, len(self.csv_data))):
                print(f"  Row {idx+1}: {self.csv_data.iloc[idx]['tumorType']}")
        
        for idx, row in self.csv_data.iterrows():
            # Process each column that we have mappings for
            for column, value in row.items():
                # Skip if this column isn't mapped or value is empty
                if column not in column_to_property_map or pd.isna(value) or value == '':
                    continue
                    
                mapped_property = column_to_property_map[column]
                valid_options = valid_values.get(mapped_property, [])
                
                if not valid_options:
                    continue
                
                # Debug output for tumorType
                if column == 'tumorType' and idx < 5:
                    print(f"Validating tumorType in row {idx+1}: '{value}'")
                    print(f"  Valid options: {[opt['label'] for opt in valid_options[:5]]}")
                
                # Check if value is valid
                is_valid = False
                
                # Handle different value types
                if pd.isna(value):
                    # Skip empty values
                    continue
                
                # Convert value to string for comparison
                value_str = str(value).lower().strip()
                
                # Handle array values in JSON format: ["value1", "value2"]
                array_values = []
                is_array = False
                
                if isinstance(value, str) and value.strip().startswith('[') and value.strip().endswith(']'):
                    try:
                        parsed = json.loads(value)
                        if isinstance(parsed, list):
                            array_values = parsed
                            is_array = True
                    except json.JSONDecodeError:
                        # Not a valid JSON array
                        pass
                        
                # Process array values
                if is_array:
                    # Check each value in the array
                    all_values_valid = True
                    for item in array_values:
                        item_str = str(item).lower().strip()
                        item_valid = False
                        
                        # Check against each valid option
                        for opt in valid_options:
                            opt_label = str(opt['label']).lower().strip()
                            opt_id = str(opt['id']).lower().strip()
                            
                            # Compare with label and id
                            if item_str == opt_label or item_str == opt_id:
                                item_valid = True
                                break
                        
                        if not item_valid:
                            all_values_valid = False
                            break
                    
                    is_valid = all_values_valid
                else:
                    # Check single value against valid options
                    for opt in valid_options:
                        opt_label = str(opt['label']).lower().strip()
                        opt_id = str(opt['id']).lower().strip()
                        
                        # Compare with label and id
                        if value_str == opt_label or value_str == opt_id:
                            is_valid = True
                            break
                
                # Debug output for validation result
                if column == 'tumorType' and idx < 5:
                    print(f"  Validation result: {'Valid' if is_valid else 'Invalid'}")
                
                # Generate suggestions if value is not valid
                if not is_valid:
                    suggested_values = self.find_best_matches(value, valid_options)
                    if suggested_values:
                        corrections.append({
                            'row_index': idx,
                            'column_name': column,
                            'original_value': value,
                            'suggested_values': suggested_values,
                            'selected_value': suggested_values[0],
                            'reason': f"Not a valid option for {column}"
                        })
        
        print(f"Generated {len(corrections)} corrections")
        self.corrections = corrections
        return corrections
    
    def apply_corrections(self, selected_corrections):
        """Apply selected corrections to the CSV data"""
        if self.csv_data is None or not selected_corrections:
            return False
            
        # Apply each correction
        for correction in selected_corrections:
            row_idx = correction['row_index']
            column = correction['column_name']
            new_value = correction['selected_value']
            
            # Update the value in the DataFrame
            self.csv_data.at[row_idx, column] = new_value
            
        return True
    
    def export_csv(self, file_path):
        """Export corrected CSV to file"""
        if self.csv_data is None:
            return False
            
        self.csv_data.to_csv(file_path, index=False)
        return True

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/corrections')
def corrections():
    return render_template('corrections.html')

@app.route('/download')
def download():
    download_path = request.args.get('path', '')
    if not download_path:
        return render_template('index.html')
    
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(os.path.basename(download_path)))
    download_url = url_for('serve_file', filename=os.path.basename(file_path))
    
    return render_template('download.html', download_url=download_url)

@app.route('/uploads/<filename>')
def serve_file(filename):
    return send_file(os.path.join(app.config['UPLOAD_FOLDER'], filename), as_attachment=True)

@app.route('/upload', methods=['POST'])
def upload_files():
    corrector = SynapseMetadataCorrector()
    
    # Handle file uploads
    if 'csv_file' not in request.files or 'jsonld_file' not in request.files:
        return jsonify({'error': 'Both CSV and JSONLD files are required'})
        
    csv_file = request.files['csv_file']
    jsonld_file = request.files['jsonld_file']
    
    # Check if files are valid
    if csv_file.filename == '' or jsonld_file.filename == '':
        return jsonify({'error': 'No file selected'})
    
    # Save uploaded files temporarily
    csv_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(csv_file.filename))
    jsonld_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(jsonld_file.filename))
    
    csv_file.save(csv_path)
    jsonld_file.save(jsonld_path)
    
    # Check if row limit was specified
    row_limit = request.form.get('row_limit', None)
    
    # Load files into corrector
    try:
        csv_rows = corrector.load_csv(csv_path, row_limit)
        jsonld_loaded = corrector.load_jsonld(jsonld_path)
        
        if not csv_rows or not jsonld_loaded:
            return jsonify({'error': 'Error loading input files'})
        
        # Generate corrections
        corrections = corrector.generate_corrections()
        
        # Save debug info
        debug_csv_columns = ', '.join(corrector.csv_data.columns.tolist())
        valid_values = corrector.extract_valid_values()
        debug_schema_properties = ', '.join(valid_values.keys())
        
        # Return results
        return jsonify({
            'status': 'success',
            'csv_rows': csv_rows,
            'correction_count': len(corrections),
            'corrections': corrections,
            'debug_csv_columns': debug_csv_columns,
            'debug_schema_properties': debug_schema_properties
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Error processing files: {str(e)}'})

@app.route('/apply-corrections', methods=['POST'])
def apply_corrections():
    corrector = SynapseMetadataCorrector()
    
    # Get data from request
    data = request.json
    if not data or 'selected_corrections' not in data:
        return jsonify({'error': 'No corrections provided'})
    
    selected_corrections = data.get('selected_corrections', [])
    if not selected_corrections:
        return jsonify({'error': 'No corrections selected'})
    
    # Check if files still exist
    csv_files = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) if f.endswith('.csv')]
    jsonld_files = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) if f.endswith('.json') or f.endswith('.jsonld')]
    
    if not csv_files or not jsonld_files:
        return jsonify({'error': 'Session expired. Please upload files again.'})
    
    csv_path = os.path.join(app.config['UPLOAD_FOLDER'], csv_files[0])
    jsonld_path = os.path.join(app.config['UPLOAD_FOLDER'], jsonld_files[0])
    
    try:
        # Load the original files
        corrector.load_csv(csv_path)
        corrector.load_jsonld(jsonld_path)
        
        # Apply corrections
        success = corrector.apply_corrections(selected_corrections)
        
        if not success:
            return jsonify({'error': 'Error applying corrections'})
        
        # Export corrected CSV
        output_filename = 'corrected_' + os.path.basename(csv_path)
        output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)
        corrector.export_csv(output_path)
        
        return jsonify({
            'status': 'success',
            'message': 'Corrections applied successfully',
            'download_path': output_filename
        })
    except Exception as e:
        return jsonify({'error': f'Error applying corrections: {str(e)}'})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)