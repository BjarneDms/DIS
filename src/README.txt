This file contains the details and run order of the src scripts.

1. Run these scripts in a virtual environment (venv) with Python version 3.12
2. In the terminal run 'pip install -r requirements.txt' to install all packages
3. Run the script 'main.py' to run:
    - network.py -> data generation
    - Part1.py -> data cleaning
    - Part2.py -> categorization
    - experiment1.py
    - experiment2.py
Any of these script can be turned off in main.py

4. All data generated from these scripts is stored in the folder of this project.
    Beware that anytime these scripts are ran, data will be generated.

5. The project comes with the data used in the experiments of the report.
    Running main.py will not be necessary for that

6. The remaining scripts serve only as scripts to be called by the ones previously mentioned.

7. In network.py you can set the length of a log in two ways.
    1. You can set the number of processes in a log
        Comment out the lines:
            290
            292
            328 until 333
        Do not comment out the line:
            291
    2. You can set the number of lines in a log
        Comment out the line:
            291
        Do not comment out the lines:
            290
            292
            328 until 333
