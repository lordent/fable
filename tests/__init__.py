from sql.app import Application


class TestApplication(Application):
    name = "test"

    def on_ready(self):
        pass
