# mqvis
HPC Slurm task queue visualization tool. Online demo is avaiable at https://umt.imm.uran.ru/mqvis.php

# running
* `python3 mqvis.py`
will print queue as text table
* `FORMAT=html python3 mqvis.py`
will print queue in HTML format. This may be called via web server for online representation.

In both cases, machine where script is run should have SLURM configured. Namely, `sinfo` and `squeue` commands are executed to achieve information about HPC cluster and it's jobs.

# copyright
2025 (c) Krasovskii Institute of Mathematics and Mechanics, Russian Academy of Sciences.
System support department, Computer visualization lab
https://parallel.uran.ru https://www.cv.imm.uran.ru
