import sys
import pandas as pd
import json
import os
import mysql.connector
from dotenv import load_dotenv
from difflib import SequenceMatcher

load_dotenv()

with open(os.path.join(os.path.dirname(__file__), '..', 'state_aliases.json'), 'r', encoding='utf-8') as state_alias_file:
    STATE_ALIASES = json.load(state_alias_file)

STATE_LOOKUP = {}
for canonical_name, aliases in STATE_ALIASES.items():
    for alias in [canonical_name, *aliases]:
        normalized_alias = ''.join(ch for ch in alias.lower() if ch.isalnum())
        STATE_LOOKUP[normalized_alias] = canonical_name


def normalize_indian_state(value):
    text = str(value or '').strip()
    if not text:
        return None

    normalized_text = ''.join(ch for ch in text.lower() if ch.isalnum())
    if normalized_text in STATE_LOOKUP:
        return STATE_LOOKUP[normalized_text]

    for alias_key, canonical_name in STATE_LOOKUP.items():
        if normalized_text == alias_key or normalized_text in alias_key or alias_key in normalized_text:
            return canonical_name

    return text.title()


def normalize_category(value):
    text = str(value or '').strip()
    if not text:
        return 'General'

    normalized_text = ' '.join(text.replace('_', ' ').replace('-', ' ').split())
    canonical_categories = {
        'electronics': 'Electronics',
        'electronic': 'Electronics',
        'furniture': 'Furniture',
        'furnitures': 'Furniture',
        'others': 'Others',
        'other': 'Others',
        'general': 'General',
        'appliances': 'Appliances',
        'appliance': 'Appliances',
        'fashion': 'Fashion',
        'clothing': 'Fashion',
        'groceries': 'Groceries',
        'grocery': 'Groceries',
        'stationery': 'Stationery',
        'books': 'Books'
    }

    lookup_key = normalized_text.lower()
    return canonical_categories.get(lookup_key, normalized_text.title())


def get_semantic_column_mapping(columns):
    semantic_groups = {
        "Product": ["item", "name", "product", "goods", "article"],
        "Revenue": ["revenue", "price", "sales", "earnings", "income", "total money", "money obtained", "received", "profit"],
        "Cost": ["cost", "expense", "expenses", "outlay", "spending", "spent"],
        "Quantity": ["quantity", "qty", "count", "amount", "volume", "units", "no of"],
        "Region": ["region", "area", "location", "state", "city", "place"],
        "Store": ["store", "shop", "entity", "outlet", "branch", "warehouse"],
        "Category": ["category", "type", "kind", "classification"]
    }

    mapping = {}

    for col in columns:
        col_normalized = col.lower()
        best_match = None
        best_score = 0.0

        for standard_col, keywords in semantic_groups.items():
            for keyword in keywords:
                if keyword in col_normalized:
                    score = len(keyword) / len(col_normalized) + 0.5
                    if score > best_score:
                        best_score = score
                        best_match = standard_col

        if best_match is None or best_score < 0.6:
            for standard_col, keywords in semantic_groups.items():
                for keyword in keywords:
                    score = SequenceMatcher(None, col_normalized, keyword).ratio()
                    if score > best_score:
                        best_score = score
                        best_match = standard_col

        if best_match and best_score > 0.6:
            mapping[col] = best_match

    return mapping


def load_dataframe(file_path):
    extension = os.path.splitext(file_path)[1].lower()
    if extension == '.csv':
        df = pd.read_csv(file_path)
    else:
        df = pd.read_excel(file_path)

    df.columns = [str(column).split("(")[0].strip().title() for column in df.columns]
    return df


def preview_file(file_path):
    try:
        df = load_dataframe(file_path)
        sample_df = df.head(5).fillna('')
        return json.dumps({
            "columns": list(df.columns),
            "sample_rows": sample_df.astype(str).to_dict(orient='records'),
            "row_count": int(len(df.index))
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


def process_excel(file_path, project_id, month, year, user_id, cursor, connection, column_mapping=None):
    print(f"Processing file: {file_path} for project: {project_id}, month: {month}, year: {year}, user: {user_id}", file=sys.stderr)
    try:
        df = load_dataframe(file_path)
        print(f"Load successful. Columns found: {list(df.columns)}", file=sys.stderr)

        if isinstance(column_mapping, dict) and len(column_mapping) > 0:
            mapping = {
                str(selected_column): str(standard_column)
                for standard_column, selected_column in column_mapping.items()
                if selected_column and str(selected_column).strip() and str(selected_column) != '__skip__'
            }
        else:
            mapping = get_semantic_column_mapping(df.columns)

        df = df.rename(columns=mapping)
        print(f"Cleaned and mapped columns: {list(df.columns)}", file=sys.stderr)
        print(f"Column mapping applied: {mapping}", file=sys.stderr)

        detected_store = None
        if 'Store' in df.columns:
            detected_store = str(df['Store'].iloc[0])
            if user_id is not None:
                cursor.execute("SELECT id FROM projects WHERE name = %s AND user_id = %s", (detected_store, user_id))
            else:
                cursor.execute("SELECT id FROM projects WHERE name = %s", (detected_store,))
            row = cursor.fetchone()
            if row:
                project_id = row[0]
            else:
                if user_id is not None:
                    cursor.execute("INSERT INTO projects (user_id, name) VALUES (%s, %s)", (user_id, detected_store))
                else:
                    cursor.execute("INSERT INTO projects (user_id, name) VALUES (%s, %s)", (1, detected_store))
                project_id = cursor.lastrowid
        else:
            if user_id is not None:
                cursor.execute("SELECT id FROM projects WHERE id = %s AND user_id = %s", (project_id, user_id))
                row = cursor.fetchone()
                if row:
                    project_id = row[0]
                else:
                    cursor.execute("SELECT id FROM projects WHERE user_id = %s ORDER BY id ASC LIMIT 1", (user_id,))
                    existing_project = cursor.fetchone()
                    if existing_project:
                        project_id = existing_project[0]
                    else:
                        cursor.execute("INSERT INTO projects (user_id, name) VALUES (%s, %s)", (user_id, "Default Project"))
                        project_id = cursor.lastrowid
            else:
                cursor.execute("SELECT id FROM projects WHERE id = %s", (project_id,))
                if not cursor.fetchone():
                    cursor.execute("INSERT INTO projects (user_id, name) VALUES (%s, %s)", (1, f"Project {project_id}"))
                    project_id = cursor.lastrowid

        required_cols = ['Product', 'Revenue', 'Cost', 'Quantity', 'Region']
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            return json.dumps({"error": f"Missing columns: {', '.join(missing)}", "columns_found": list(df.columns)})

        if 'Category' not in df.columns:
            df['Category'] = 'General'

        df['Revenue'] = pd.to_numeric(df['Revenue'], errors='coerce').fillna(0)
        df['Cost'] = pd.to_numeric(df['Cost'], errors='coerce').fillna(0)
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0)
        df['Region'] = df['Region'].apply(normalize_indian_state)
        df['Category'] = df['Category'].apply(normalize_category)
        df['Product'] = df['Product'].fillna('').astype(str).str.strip()

        df['Net Revenue'] = df['Revenue'] - df['Cost']
        total_revenue = float(df['Revenue'].sum())
        total_cost = float(df['Cost'].sum())
        total_net_revenue = float(df['Net Revenue'].sum())
        total_quantity = int(df['Quantity'].sum())

        category_data = {str(k): float(v) for k, v in df.groupby('Category')['Revenue'].sum().to_dict().items()}
        region_data = {str(k): float(v) for k, v in df.groupby('Region')['Revenue'].sum().to_dict().items()}
        top_products = {str(k): float(v) for k, v in df.groupby('Product')['Revenue'].sum().sort_values(ascending=False).to_dict().items()}
        detailed_entries_df = (
            df.groupby(['Product', 'Category', 'Region'], dropna=False, as_index=False)
              .agg({'Revenue': 'sum', 'Cost': 'sum', 'Quantity': 'sum'})
        )
        detailed_entries = []
        for _, detail_row in detailed_entries_df.iterrows():
            detailed_entries.append({
                "product": str(detail_row.get('Product', '')).strip(),
                "category": str(detail_row.get('Category', 'General')).strip() or 'General',
                "region": str(detail_row.get('Region', 'Unknown')).strip() or 'Unknown',
                "revenue": float(detail_row.get('Revenue', 0) or 0),
                "cost": float(detail_row.get('Cost', 0) or 0),
                "quantity": int(detail_row.get('Quantity', 0) or 0)
            })

        top_product = list(top_products.keys())[0] if top_products else "N/A"
        top_region = list(region_data.keys())[0] if region_data else "N/A"

        profit_margin = (total_net_revenue / total_revenue * 100) if total_revenue > 0 else 0
        insight = f"Performance in {top_region} is leading! {top_product} is your primary growth driver with a {profit_margin:.1f}% net margin."

        sql = """
        INSERT INTO sales_summaries (project_id, month_name, year, total_revenue, total_cost, net_revenue, total_quantity, top_product, top_region, insight, region_data, category_data, top_products, detailed_entries)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        total_revenue = VALUES(total_revenue), total_cost = VALUES(total_cost), net_revenue = VALUES(net_revenue),
        total_quantity = VALUES(total_quantity), top_product = VALUES(top_product), top_region = VALUES(top_region),
        insight = VALUES(insight), region_data = VALUES(region_data), category_data = VALUES(category_data), top_products = VALUES(top_products), detailed_entries = VALUES(detailed_entries)
        """
        cursor.execute(sql, (
            project_id, month, year, total_revenue, total_cost, total_net_revenue, total_quantity,
            top_product, top_region, insight, json.dumps(region_data), json.dumps(category_data), json.dumps(top_products), json.dumps(detailed_entries)
        ))
        connection.commit()

        return json.dumps({"project_id": project_id, "month": month, "year": year, "total_revenue": total_revenue, "region_data": region_data, "category_data": category_data})

    except Exception as e:
        return json.dumps({"error": str(e)})


def main():
    if len(sys.argv) < 3:
        print(json.dumps({"error": "Missing arguments"}))
        return

    command = sys.argv[1]

    if command == 'preview':
        print(preview_file(sys.argv[2]))
        return

    if command == 'process':
        if len(sys.argv) < 7:
            print(json.dumps({"error": "Missing arguments (filePath, projectId, month, year, userId, columnMapping)"}))
            return

        file_path, project_id, month, year, user_id = sys.argv[2:7]
        raw_mapping = sys.argv[7] if len(sys.argv) > 7 else '{}'
    else:
        if len(sys.argv) < 5:
            print(json.dumps({"error": "Missing arguments (filePath, projectId, month, year)"}))
            return

        file_path, project_id, month, year = sys.argv[1:5]
        user_id = sys.argv[5] if len(sys.argv) > 5 else None
        raw_mapping = '{}'

    try:
        column_mapping = json.loads(raw_mapping or '{}')
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            user=os.getenv('DB_USER', 'user'),
            password=os.getenv('DB_PASSWORD', 'password'),
            database=os.getenv('DB_NAME', 'datavis_db')
        )
        cursor = connection.cursor()
        result = process_excel(file_path, project_id, month, year, user_id, cursor, connection, column_mapping)
        print(result)
        connection.commit()
    except Exception as e:
        print(json.dumps({"error": str(e)}))
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()


if __name__ == "__main__":
    main()
