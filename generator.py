from jinja2 import Environment, FileSystemLoader
import yaml
import os

file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))

for filename in os.listdir(f'{file_dir}/config'):
    if filename.endswith('.yaml'):
        with open(f'{file_dir}/config/{filename}', 'r') as configfile:
            config = yaml.safe_load(configfile)
            template_name = config['dag_template']
            template = env.get_template(f'./templates/{template_name}.jinja2')
            with open(f'dags/{config["dag_id"]}.py', 'w') as f:
                f.write(template.render(config))