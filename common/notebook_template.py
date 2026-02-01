import nbformat
from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell
import os
import logging

logging.basicConfig(
    level = logging.INFO,
    # format = "%(levelname)s: %(name)s: %(message)s"  ## Does not really do anything different from default
    )
logger = logging.getLogger('log_notebook_template')
class Notebook:
    def __init__(
            self,
            notebook_root_path: str,
            notebook_name: str,
            ):
        self.notebook_root_path = notebook_root_path
        self.notebook_name = f'{notebook_name}.ipynb'
        self.notebook_path = f'{self.notebook_root_path}/{self.notebook_name}'
    
    def check_notebook_exists(self):
        logger.info(f'Check if {self.notebook_name} exists')
        check_result = os.path.exists(f"{self.notebook_path}")
        logger.info(f'check_result={check_result}')
        return check_result
        
    def create_notebook(self):
        if self.check_notebook_exists():
            pass
        else:
            logger.info(f'Create new notebook {self.notebook_name}')
            nb = new_notebook()
            with open(f'{self.notebook_path}', 'w') as f:
                nbformat.write(nb, f)

    def inspect_notebook_cell (
            self, 
            cell_name=None):
        nb = nbformat.read(f"{self.notebook_path}", as_version = 4)
        for cell in nb.cells:
            print(cell.cell_type)
            print(cell.id)
            print(cell.source, "\n")
        return

    def edit_cell_content(
            self,
            cell_contents:list,
            cell_prefix = None
            ):
        nb = nbformat.read(f"{self.notebook_path}", as_version = 4)
        # empty notebook before write
        logger.info('Empty notebook cells')
        nb.cells=[]
        nbformat.write(nb, self.notebook_path)
        # write cells
        logger.info('Write to notebook cells')
        for cell_content in cell_contents:
            nb.cells.append(new_code_cell(f"{cell_prefix}\n{ cell_content }"))
        #save notebook
        nbformat.write(nb, self.notebook_path)