class Plumbing:
    """

    """
    def __init(self,
               config=None):
        """

        """
        self.config = config
        self.pipeline_steps = self.get_pipeline()

    def get_pipeline(self):
        """

        :return:
        """
        return self.config.get("pipe")
