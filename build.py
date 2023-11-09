import os
import pathlib
import shutil
import yaml

def copy_rules(basedir):
	with open(os.path.join(basedir, 'version_sets.yaml'), 'r') as file:
		version_sets = yaml.safe_load(file)

	for version, base_files in version_sets.items():
		dest = os.path.join(basedir, 'version_sets', version)
		if os.path.exists(dest):
			shutil.rmtree(dest)
		pathlib.Path(dest).mkdir(parents=True)
		for f in base_files:
			src_file = os.path.join(basedir, 'base_rule_definitions', f)
			dest_file = os.path.join(dest, f)
			shutil.copyfile(src_file, dest_file)

if __name__ == "__main__":
	copy_rules(basedir='focus_validator/rules')
