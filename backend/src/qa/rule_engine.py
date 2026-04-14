"""
Rule Engine for configurable data validation.
Loads rules from validation_rules.json and applies them to dataframe columns.
Includes automatic format detection for columns without explicit rules.
"""

import re
import json
import os
import pandas as pd
from urllib.parse import urlparse
from collections import Counter


# Common format patterns for auto-detection
FORMAT_PATTERNS = {
    'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    'url': r'^https?://[^\s]+$',
    'phone_intl': r'^\+[1-9]\d{1,14}$',
    'phone_domestic': r'^[0-9]{10,11}$',
    'phone_formatted': r'^[\d\s\-().+]{7,20}$',
    'date_iso': r'^\d{4}-\d{2}-\d{2}$',
    'date_us': r'^\d{1,2}/\d{1,2}/\d{2,4}$',
    'date_eu': r'^\d{1,2}-\d{1,2}-\d{2,4}$',
    'datetime_iso': r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}',
    'uuid': r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    'ip_address': r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$',
    'code_alpha_num': r'^[A-Z]{2,4}-\d{2,6}$',  # e.g., ABC-1234, EMP-001
    'code_prefix_num': r'^[A-Z]+\d+$',  # e.g., ABC123, EMP001
    'currency': r'^[$€£¥]\s?[\d,]+\.?\d*$',
    'percentage': r'^\d+\.?\d*\s?%$',
    'zip_us': r'^\d{5}(-\d{4})?$',
    'zip_uk': r'^[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}$',
    'boolean_text': r'^(true|false|yes|no|y|n|1|0)$',
    'integer': r'^-?\d+$',
    'decimal': r'^-?\d+\.\d+$',
    'alphanumeric': r'^[A-Za-z0-9]+$',
    'alpha_only': r'^[A-Za-z]+$',
}


class FormatDetector:
    """
    Automatically detects the dominant format/pattern in a column.
    Used for columns without explicit validation rules configured.
    """
    
    @staticmethod
    def detect_format(values):
        """
        Detect the dominant format pattern in a list of values.
        
        Args:
            values: list/series of values to analyze
            
        Returns:
            dict: {
                'detected_format': str (format name or 'custom'),
                'pattern': str (regex pattern),
                'confidence': float (% of values matching),
                'sample_match': str (example value that matches)
            }
        """
        # Filter out null/empty values
        clean_values = []
        for v in values:
            if pd.notna(v):
                str_v = str(v).strip()
                if str_v:
                    clean_values.append(str_v)
        
        if not clean_values:
            return {
                'detected_format': 'empty',
                'pattern': None,
                'confidence': 100.0,
                'sample_match': None
            }
        
        # Check each known format pattern
        format_matches = {}
        for fmt_name, pattern in FORMAT_PATTERNS.items():
            match_count = sum(1 for v in clean_values if re.match(pattern, v, re.IGNORECASE))
            if match_count > 0:
                format_matches[fmt_name] = {
                    'count': match_count,
                    'pct': (match_count / len(clean_values)) * 100,
                    'pattern': pattern
                }
        
        # Find the format with highest match percentage (must be > 50% to be considered dominant)
        if format_matches:
            best_format = max(format_matches.items(), key=lambda x: x[1]['pct'])
            if best_format[1]['pct'] >= 50:
                # Find a sample that matches
                sample = next((v for v in clean_values if re.match(best_format[1]['pattern'], v, re.IGNORECASE)), None)
                return {
                    'detected_format': best_format[0],
                    'pattern': best_format[1]['pattern'],
                    'confidence': best_format[1]['pct'],
                    'sample_match': sample
                }
        
        # No known format found - try to infer custom pattern from data
        custom_pattern = FormatDetector._infer_custom_pattern(clean_values)
        if custom_pattern:
            return custom_pattern
        
        # Fallback: no consistent format detected
        return {
            'detected_format': 'mixed',
            'pattern': None,
            'confidence': 0.0,
            'sample_match': None
        }
    
    @staticmethod
    def _infer_custom_pattern(values):
        """
        Try to infer a custom pattern when no standard format matches.
        Looks for structural similarities (same length, same character positions, etc.)
        """
        if len(values) < 3:
            return None
        
        # Sample values for pattern inference
        sample_size = min(50, len(values))
        samples = values[:sample_size]
        
        # Check if all values have same length
        lengths = [len(v) for v in samples]
        length_counter = Counter(lengths)
        most_common_length, length_count = length_counter.most_common(1)[0]
        
        if length_count / len(samples) >= 0.7:
            # Most values have same length - try to build character-class pattern
            same_len_values = [v for v in samples if len(v) == most_common_length]
            pattern = FormatDetector._build_char_pattern(same_len_values)
            if pattern:
                # Validate pattern against all values
                match_count = sum(1 for v in values if re.match(pattern, v))
                confidence = (match_count / len(values)) * 100
                
                if confidence >= 60:
                    return {
                        'detected_format': 'custom',
                        'pattern': pattern,
                        'confidence': confidence,
                        'sample_match': same_len_values[0] if same_len_values else None
                    }
        
        return None
    
    @staticmethod
    def _build_char_pattern(values):
        """
        Build a regex pattern based on character positions in values.
        e.g., ['ABC-123', 'XYZ-456'] -> '^[A-Z]{3}-\\d{3}$'
        """
        if not values:
            return None
        
        length = len(values[0])
        pattern_parts = []
        
        for pos in range(length):
            chars_at_pos = [v[pos] for v in values if len(v) > pos]
            if not chars_at_pos:
                break
            
            # Determine character class at this position
            all_digits = all(c.isdigit() for c in chars_at_pos)
            all_upper = all(c.isupper() for c in chars_at_pos)
            all_lower = all(c.islower() for c in chars_at_pos)
            all_alpha = all(c.isalpha() for c in chars_at_pos)
            all_same = len(set(chars_at_pos)) == 1
            
            if all_same:
                # Fixed character at this position
                char = chars_at_pos[0]
                if char in r'\.^$*+?{}[]|()':
                    pattern_parts.append('\\' + char)
                else:
                    pattern_parts.append(re.escape(char))
            elif all_digits:
                pattern_parts.append(r'\d')
            elif all_upper:
                pattern_parts.append('[A-Z]')
            elif all_lower:
                pattern_parts.append('[a-z]')
            elif all_alpha:
                pattern_parts.append('[A-Za-z]')
            else:
                pattern_parts.append('.')
        
        if pattern_parts:
            # Compress consecutive same patterns
            compressed = FormatDetector._compress_pattern(pattern_parts)
            return '^' + compressed + '$'
        
        return None
    
    @staticmethod
    def _compress_pattern(parts):
        """Compress repeated pattern parts: [\\d, \\d, \\d] -> \\d{3}"""
        if not parts:
            return ''
        
        result = []
        current = parts[0]
        count = 1
        
        for part in parts[1:]:
            if part == current:
                count += 1
            else:
                if count > 1:
                    result.append(f'{current}{{{count}}}')
                else:
                    result.append(current)
                current = part
                count = 1
        
        # Handle last group
        if count > 1:
            result.append(f'{current}{{{count}}}')
        else:
            result.append(current)
        
        return ''.join(result)


class RuleEngine:
    """
    Applies configurable validation rules to dataframe columns.
    Rules are loaded from validation_rules.json and affect the Validity dimension.
    """
    
    def __init__(self, rules_file=None):
        """Initialize the rule engine with rules from config file."""
        if rules_file is None:
            # Default to validation_rules.json in same directory
            rules_file = os.path.join(os.path.dirname(__file__), 'validation_rules.json')
        
        self.rules = []
        self.pattern_aliases = {}
        self._load_rules(rules_file)
    
    def _load_rules(self, rules_file):
        """Load rules from JSON config file."""
        if os.path.exists(rules_file):
            try:
                with open(rules_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    self.rules = [r for r in config.get('rules', []) if r.get('enabled', True)]
                    self.pattern_aliases = config.get('pattern_aliases', {})
                    print(f"[*] Rule Engine: Loaded {len(self.rules)} validation rules")
            except Exception as e:
                print(f"[!] Rule Engine: Error loading rules - {e}")
                self.rules = []
        else:
            print(f"[*] Rule Engine: No rules file found at {rules_file}")
    
    def get_rules_for_column(self, column_name):
        """Get all rules that apply to a specific column (case-insensitive match)."""
        matching_rules = []
        col_lower = column_name.lower()
        
        for rule in self.rules:
            rule_col = rule.get('column', '').lower()
            # Match exact column name or if column contains the rule target
            # e.g., rule for "email" matches columns like "user_email", "email_address"
            if rule_col == col_lower or rule_col in col_lower or col_lower in rule_col:
                matching_rules.append(rule)
        
        return matching_rules
    
    def validate_value(self, value, rule):
        """
        Validate a single value against a rule.
        Returns True if valid, False if invalid.
        """
        rule_type = rule.get('type', '')
        
        # Handle null/empty values
        if pd.isna(value) or value is None:
            if rule_type == 'not_null':
                return False
            # For other rules, skip null values (let Completeness handle them)
            return True
        
        str_value = str(value).strip()
        
        # Handle empty strings
        if not str_value:
            if rule_type == 'not_null':
                return False
            return True
        
        try:
            if rule_type == 'regex':
                pattern = rule.get('pattern', '')
                # Check for pattern alias
                if pattern in self.pattern_aliases:
                    pattern = self.pattern_aliases[pattern]
                return bool(re.match(pattern, str_value, re.IGNORECASE))
            
            elif rule_type == 'not_null':
                # Already handled above for null/empty
                return True
            
            elif rule_type == 'range':
                try:
                    num_value = float(value)
                    min_val = rule.get('min', float('-inf'))
                    max_val = rule.get('max', float('inf'))
                    return min_val <= num_value <= max_val
                except (ValueError, TypeError):
                    return False
            
            elif rule_type == 'enum':
                allowed_values = rule.get('values', [])
                # Case-insensitive comparison
                str_lower = str_value.lower()
                return any(str_lower == v.lower() for v in allowed_values)
            
            elif rule_type == 'date_format':
                from datetime import datetime
                fmt = rule.get('format', '%Y-%m-%d')
                try:
                    datetime.strptime(str_value, fmt)
                    return True
                except ValueError:
                    return False
            
            elif rule_type == 'length':
                min_len = rule.get('min_len', 0)
                max_len = rule.get('max_len', float('inf'))
                return min_len <= len(str_value) <= max_len
            
            elif rule_type == 'email':
                # Basic email validation
                email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                return bool(re.match(email_pattern, str_value))
            
            elif rule_type == 'url':
                try:
                    result = urlparse(str_value)
                    return all([result.scheme, result.netloc])
                except Exception:
                    return False
            
            elif rule_type == 'phone':
                # Default phone pattern - allows various formats
                pattern = rule.get('pattern', r'^[+]?[0-9\s\-().]{7,20}$')
                if pattern in self.pattern_aliases:
                    pattern = self.pattern_aliases[pattern]
                return bool(re.match(pattern, str_value))
            
            else:
                # Unknown rule type - pass validation
                return True
                
        except Exception as e:
            print(f"[!] Rule Engine: Error validating value - {e}")
            return True  # On error, don't penalize
    
    def validate_column(self, df, column_name):
        """
        Validate all values in a column against applicable rules.
        
        Returns:
            dict: {
                'compliance_rate': float (0-100),
                'rules_applied': int,
                'violations': list of dicts with details
            }
        """
        rules = self.get_rules_for_column(column_name)
        
        if not rules or column_name not in df.columns:
            return {
                'compliance_rate': 100.0,
                'rules_applied': 0,
                'violations': []
            }
        
        total_checks = 0
        passed_checks = 0
        violations = []
        
        col_values = df[column_name]
        
        for rule in rules:
            for idx, value in col_values.items():
                total_checks += 1
                if self.validate_value(value, rule):
                    passed_checks += 1
                else:
                    violations.append({
                        'row_index': idx,
                        'rule_type': rule.get('type'),
                        'value': str(value)[:50],  # Truncate for brevity
                        'message': rule.get('message', 'Validation failed')
                    })
        
        compliance_rate = (passed_checks / total_checks * 100.0) if total_checks > 0 else 100.0
        
        return {
            'compliance_rate': compliance_rate,
            'rules_applied': len(rules),
            'violations': violations[:10]  # Limit to first 10 for brevity
        }
    
    def auto_detect_format(self, df, column_name):
        """
        Automatically detect and validate format consistency for a column.
        Used for columns WITHOUT explicit rules configured.
        
        Returns:
            dict: {
                'compliance_rate': float (0-100),
                'detected_format': str,
                'pattern': str or None,
                'confidence': float,
                'non_compliant_count': int
            }
        """
        if column_name not in df.columns:
            return {
                'compliance_rate': 100.0,
                'detected_format': 'unknown',
                'pattern': None,
                'confidence': 0.0,
                'non_compliant_count': 0
            }
        
        col_values = df[column_name]
        
        # Detect the dominant format
        detection = FormatDetector.detect_format(col_values.tolist())
        
        if detection['pattern'] is None or detection['detected_format'] in ('empty', 'mixed'):
            # No clear format detected - can't validate, return 100%
            return {
                'compliance_rate': 100.0,
                'detected_format': detection['detected_format'],
                'pattern': None,
                'confidence': detection['confidence'],
                'non_compliant_count': 0
            }
        
        # Validate all non-null values against detected pattern
        pattern = detection['pattern']
        total = 0
        compliant = 0
        non_compliant_values = []
        
        for idx, value in col_values.items():
            if pd.notna(value):
                str_val = str(value).strip()
                if str_val:  # Skip empty strings
                    total += 1
                    if re.match(pattern, str_val, re.IGNORECASE):
                        compliant += 1
                    else:
                        if len(non_compliant_values) < 5:
                            non_compliant_values.append(str_val[:30])
        
        compliance_rate = (compliant / total * 100.0) if total > 0 else 100.0
        
        return {
            'compliance_rate': compliance_rate,
            'detected_format': detection['detected_format'],
            'pattern': pattern,
            'confidence': detection['confidence'],
            'non_compliant_count': total - compliant,
            'non_compliant_samples': non_compliant_values
        }
    
    def validate_dataframe(self, df, exclude_cols=None):
        """
        Validate all columns in a dataframe against configured rules.
        For columns WITHOUT rules, uses automatic format detection.
        
        Args:
            df: pandas DataFrame to validate
            exclude_cols: set of column names to skip
            
        Returns:
            dict: {
                'overall_compliance': float (0-100),
                'columns_validated': int,
                'column_results': {col: result_dict},
                'auto_detected_columns': int
            }
        """
        if exclude_cols is None:
            exclude_cols = {'source', 'ingested_at', 'remediation_notes'}
        
        data_cols = [c for c in df.columns if c not in exclude_cols]
        
        column_results = {}
        total_compliance = 0.0
        columns_validated = 0
        auto_detected_count = 0
        
        for col in data_cols:
            # Check if column has configured rules
            rules = self.get_rules_for_column(col)
            
            if rules:
                # Use configured rules
                result = self.validate_column(df, col)
                result['validation_type'] = 'configured_rules'
                column_results[col] = result
                
                if result['rules_applied'] > 0:
                    total_compliance += result['compliance_rate']
                    columns_validated += 1
                    
                    if result['compliance_rate'] < 100:
                        print(f"    [Rule Engine] '{col}': {result['compliance_rate']:.1f}% compliance ({result['rules_applied']} configured rules)")
            else:
                # No configured rules - use automatic format detection
                auto_result = self.auto_detect_format(df, col)
                auto_result['validation_type'] = 'auto_detected'
                auto_result['rules_applied'] = 0
                column_results[col] = auto_result
                
                # Only count if a format was detected (not mixed/empty)
                if auto_result['detected_format'] not in ('empty', 'mixed', 'unknown'):
                    total_compliance += auto_result['compliance_rate']
                    columns_validated += 1
                    auto_detected_count += 1
                    
                    if auto_result['compliance_rate'] < 100:
                        non_compliant = auto_result.get('non_compliant_count', 0)
                        samples = auto_result.get('non_compliant_samples', [])
                        sample_str = f" (e.g., '{samples[0]}')" if samples else ""
                        print(f"    [Auto-Format] '{col}': {auto_result['compliance_rate']:.1f}% follow detected format '{auto_result['detected_format']}' ({non_compliant} deviations{sample_str})")
        
        overall_compliance = (total_compliance / columns_validated) if columns_validated > 0 else 100.0
        
        if auto_detected_count > 0:
            print(f"    [Auto-Format] Detected formats for {auto_detected_count} columns without configured rules")
        
        return {
            'overall_compliance': overall_compliance,
            'columns_validated': columns_validated,
            'column_results': column_results,
            'auto_detected_columns': auto_detected_count
        }


# Convenience function
def apply_rules(df, exclude_cols=None):
    """
    Apply validation rules to a dataframe.
    Returns compliance rate (0-100).
    """
    engine = RuleEngine()
    result = engine.validate_dataframe(df, exclude_cols)
    return result['overall_compliance']
