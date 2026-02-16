from src.utils.csv_reader import CSVReader


class Pipeline:

    def __init__(self, data_path, start, end):
        self.data_path = data_path
        self.start = start
        self.end = end
        self.data = None

    def run(self):
        print("Running pipeline")

        self.load_data()

        # später:
        # self.compute_features()
        # self.generate_signals()
        # self.backtest()

    def load_data(self):
        reader = CSVReader(self.data_path)

        self.data = reader.load_range(
            start=self.start,
            end=self.end
        )

        print(self.data.head())
