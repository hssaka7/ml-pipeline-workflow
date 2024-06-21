import logging
from core.step import Step, FileState

logger = logging.getLogger(__name__)

class ExtractDropzone(Step):
    def run(self):
        logger.info("Extracting files form dropzone")
        files_to_load = self.config['files']
        logger.info(files_to_load)

class ReadSnowflake(Step):
    def run(self):
        pass

class SendEmail(Step):
    def run(self):
        pass