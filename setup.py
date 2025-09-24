from setuptools import setup, find_packages
import os

def banda_ki_setup():
    # Step/1: Initialize project configuration
    print("Step/1: Starting fresh setup.py configuration...")
    project_name = "my-project"
    version = "0.1.0"
    description = "A Python project with Flask static files"
    print(f"Project initialized: {project_name} v{version}")

    # Step/2: Collect basic requirements
    print("Step/2: Setting up basic dependencies...")
    basic_requirements = [
        "requests>=2.28.0",
        "python-dotenv>=0.19.0",
        "flask>=2.0.0",
        "pinecone-client>=2.0.0",
        "openai>=1.0.0",
        "numpy>=1.20.0",
        "pandas>=1.3.0"
    ]
    print(f"Basic requirements: {len(basic_requirements)} packages")

    # Step/3: Setup development dependencies
    print("Step/3: Configuring development tools...")
    dev_requirements = [
        "pytest>=7.0.0",
        "black>=22.0.0",
        "flake8>=5.0.0"
    ]
    print(f"Development tools: {len(dev_requirements)} packages")

    # Step/4: Define project metadata
    print("Step/4: Setting project metadata...")
    author_name = "Your Name"
    author_email = "your.email@example.com"
    project_url = "https://github.com/yourusername/my-project"
    print(f"Author: {author_name}")
    print(f"URL: {project_url}")

    # Step/5: Setup package discovery
    print("Step/5: Configuring package discovery...")
    packages = find_packages()
    print(f"Found packages: {packages}")

    # Step/6: Configure entry points
    print("Step/6: Setting up entry points...")
    entry_points = {
        'console_scripts': [
            'my-project=main:main',
        ]
    }
    print("Console scripts configured")

    # Step/7: Define classifiers
    print("Step/7: Setting up PyPI classifiers...")
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent"
    ]
    print(f"Classifiers: {len(classifiers)} items")

    # Step/8: Handle README file
    print("Step/8: Checking for README file...")
    long_description = description
    readme_path = "README.md"
    print(f"Looking for {readme_path}...")
    long_description = open(readme_path).read() if os.path.exists(readme_path) else description
    print(f"Long description source: {'README.md' if os.path.exists(readme_path) else 'fallback'}")

    # Step/9: Configure package data for static files - BANDA KI CRITICAL FIX
    print("Step/9: Configuring package data for static and template files...")
    
    package_data = {}
    
    # Step/9.1: Check for static files
    print("Step/9.1: Scanning for static files...")
    static_dir = "static"
    if os.path.exists(static_dir):
        static_files = []
        for root, dirs, files in os.walk(static_dir):
            for file in files:
                rel_path = os.path.relpath(os.path.join(root, file))
                static_files.append(rel_path)
        
        if static_files:
            package_data[''] = static_files
            print(f"Found {len(static_files)} static files:")
            for file in static_files:
                print(f"  - {file}")
    else:
        print("No static directory found")
    
    # Step/9.2: Check for template files
    print("Step/9.2: Scanning for template files...")
    template_dir = "templates"
    if os.path.exists(template_dir):
        template_files = []
        for root, dirs, files in os.walk(template_dir):
            for file in files:
                rel_path = os.path.relpath(os.path.join(root, file))
                template_files.append(rel_path)
        
        if template_files:
            if '' in package_data:
                package_data[''].extend(template_files)
            else:
                package_data[''] = template_files
            print(f"Found {len(template_files)} template files:")
            for file in template_files:
                print(f"  - {file}")
    else:
        print("No templates directory found")
    
    print(f"Total package data files: {len(package_data.get('', []))}")

    # Step/10: Create MANIFEST.in file for additional file inclusion
    print("Step/10: Creating MANIFEST.in file...")
    manifest_content = """# BANDA KI MANIFEST.IN - Include static and template files
include README.md
include requirements.txt
include .env
recursive-include static *
recursive-include templates *
recursive-include main_line *
global-exclude *.pyc
global-exclude __pycache__
global-exclude .DS_Store
"""
    
    print("Step/10.1: Writing MANIFEST.in file...")
    with open("MANIFEST.in", "w") as f:
        f.write(manifest_content)
    print("MANIFEST.in file created successfully")

    # Step/11: Execute setup configuration
    print("Step/11: Running setup configuration...")
    setup(
        name=project_name,
        version=version,
        description=description,
        long_description=long_description,
        long_description_content_type="text/markdown",
        
        author=author_name,
        author_email=author_email,
        
        url=project_url,
        project_urls={
            "Bug Tracker": f"{project_url}/issues",
            "Source Code": project_url,
        },
        
        packages=packages,
        
        # Step/11.1: BANDA KI CRITICAL - Include package data for static files
        package_data=package_data,
        include_package_data=True,
        
        install_requires=basic_requirements,
        extras_require={
            'dev': dev_requirements,
        },
        
        python_requires='>=3.8',
        
        entry_points=entry_points,
        
        classifiers=classifiers,
        keywords="python, project, flask, static-files",
        license="MIT",
        
        zip_safe=False,  # Critical for static files
    )

    print("Step/12: Setup configuration complete!")
    print("Installation commands:")
    print("  Development mode: pip install -e .")
    print("  With dev deps: pip install -e .[dev]")
    print("  Build package: python setup.py sdist bdist_wheel")
    print("")
    print("BANDA KI Static Files Setup:")
    print("  1. Ensure static/hei_logo.png exists")
    print("  2. Ensure templates/index.html exists") 
    print("  3. Run: pip install -e .")
    print("  4. Test with: python main.py")
    print("  5. Visit: http://localhost:5000/debug-static")

if __name__ == '__main__':
    banda_ki_setup()