import os
import json
import logging
from subprocess import run, PIPE

# Constants
CONFIG_FILE = "config.json"

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def load_config(config_file):
    """Load the configuration from a JSON file."""
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file '{config_file}' not found.")
    with open(config_file, "r") as f:
        config = json.load(f)
    required_keys = ["base_dir", "project_dir", "app_dir", "database_alias"]
    for key in required_keys:
        if key not in config:
            raise KeyError(f"Missing required configuration key: '{key}'")
    return config

def run_inspectdb(project_path, database_alias):
    """Run inspectdb for a specified database and return its output."""
    command = ["python", os.path.join(project_path, "manage.py"), "inspectdb"]
    if database_alias:
        command.extend(["--database", database_alias])
    result = run(command, stdout=PIPE, stderr=PIPE, text=True, cwd=project_path)
    if result.returncode != 0:
        raise RuntimeError(f"inspectdb failed: {result.stderr}")
    return result.stdout

#def append_models(app_path, inspectdb_output):
    #"""Append models from inspectdb to the models.py file."""
    #models_file = os.path.join(app_path, "models.py")
    #with open(models_file, "w") as f:
        #f.write("\n\n# Models generated by inspectdb\n")
        #f.write(inspectdb_output)

import os


def append_models(app_path, inspectdb_output):
    """Append models from inspectdb to the models.py file."""
    models_file = os.path.join(app_path, "models.py")

    # Check if the first row contains 'dna_id = models.Int32Field()' and update it
    if "dna_id = models.Int32Field()" in inspectdb_output:
        inspectdb_output = inspectdb_output.replace(
            "dna_id = models.Int32Field()",
            "dna_id = models.Int32Field(primary_key=True)"
        )

    # Append the updated models to the models.py file
    with open(models_file, "a") as f:  # Use 'a' to append instead of 'w' to overwrite
        f.write("\n\n# Models generated by inspectdb\n")
        f.write(inspectdb_output)

def generate_serializers(app_path, model_names):
    """Generate serializers for the models."""
    serializers_file = os.path.join(app_path, "serializers.py")
    serializers_code = "from rest_framework import serializers\nfrom .models import (\n"
    for model in model_names:
        serializers_code += f"    {model},\n"
    serializers_code = serializers_code.rstrip(",\n") + "\n)\n\n"
    for model in model_names:
        serializers_code += f"""
class {model}Serializer(serializers.ModelSerializer):
    class Meta:
        model = {model}
        fields = '__all__'\n"""
    with open(serializers_file, "w") as f:
        f.write(serializers_code)

def generate_views(app_path, model_names):
    """Generate views for the models."""
    views_file = os.path.join(app_path, "views.py")
    views_code = "from rest_framework import viewsets\nfrom .models import (\n"
    for model in model_names:
        views_code += f"    {model},\n"
    views_code = views_code.rstrip(",\n") + "\n)\n\n"
    views_code += "from .serializers import (\n"
    for model in model_names:
        views_code += f"    {model}Serializer,\n"
    views_code = views_code.rstrip(",\n") + "\n)\n\n"
    for model in model_names:
        views_code += f"""
class {model}ViewSet(viewsets.ModelViewSet):
    queryset = {model}.objects.all()
    serializer_class = {model}Serializer\n"""
    with open(views_file, "w") as f:
        f.write(views_code)

def update_urls(app_path, model_names):
    """Update urls.py with new API endpoints."""
    urls_file = os.path.join(app_path, "urls.py")
    url_code = "from django.urls import path, include\nfrom rest_framework.routers import DefaultRouter\nfrom .views import (\n"
    for model in model_names:
        url_code += f"    {model}ViewSet,\n"
    url_code = url_code.rstrip(",\n") + "\n)\n\n"
    url_code += "router = DefaultRouter()\n"
    for model in model_names:
        url_code += f"router.register(r'{model.lower()}', {model}ViewSet, basename='{model.lower()}')\n"
    url_code += "\nurlpatterns = [\n    path('', include(router.urls)),\n]\n"

    # Ensure the file is created if it doesn't exist
    if not os.path.exists(urls_file):
        with open(urls_file, "w") as f:
            f.write("from django.urls import path, include\n\n")

    with open(urls_file, "w") as f:
        f.write(url_code)

def extract_model_names(inspectdb_output):
    """
    Extract model names from the inspectdb output.
    Returns a list of model names like ['Caca', 'Simple', 'Simple10'].
    """
    model_names = []
    for line in inspectdb_output.splitlines():
        if line.startswith("class "):  # Match only class definitions
            # Extract the class name by splitting the line and removing the parenthesis
            model_name = line.split()[1].split("(")[0]
            model_names.append(model_name)
    return model_names

def main():
    try:
        config = load_config(CONFIG_FILE)
        base_dir = config["base_dir"]
        project_dir = config["project_dir"]
        app_dir = config["app_dir"]
        database_alias = config["database_alias"]

        project_path = os.path.join(base_dir, project_dir)
        app_path = os.path.join(project_path, app_dir)

        # Validate paths
        if not os.path.isdir(project_path):
            raise FileNotFoundError(f"Project directory '{project_path}' does not exist.")
        if not os.path.isdir(app_path):
            raise FileNotFoundError(f"App directory '{app_path}' does not exist.")

        # Run the process
        inspectdb_output = run_inspectdb(project_path, database_alias)
        model_names = extract_model_names(inspectdb_output)
        append_models(app_path, inspectdb_output)
        generate_serializers(app_path, model_names)
        generate_views(app_path, model_names)
        update_urls(app_path, model_names)
        logging.info("API endpoints generated successfully!")

    except Exception as e:
        logging.error(f"Error: {e}")

if __name__ == "__main__":
    main()
