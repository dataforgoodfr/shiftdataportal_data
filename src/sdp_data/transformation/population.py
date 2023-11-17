

class GapMinderCleaner:

    def __init__(self):
        self.equivalence_dict = {'k': 1e3, 'M': 1e6, 'B': 1e9}
        self.max_year = 2021

    def dirty_string_to_int(self, dirty_string: str):
        """
        Cleans values such as 3.35M into 3350000.
        :param dirty_string: (str) the string to convert in integer
        :return:
        """
        for key in self.equivalence_dict.keys():
            if key in dirty_string:
                dirty_string = dirty_string.replace(key, '')
                units = float(dirty_string) * self.equivalence_dict[key]
                return int(units)

    @staticmethod
    def unstack_dataframe_to_serie(df):
        df = df.unstack().reset_index()
        df.columns = ["year", "country", "population"]
        return df

    def run(self, df):
        """

        :return:
        """
        # clean the numbers
        df = df.applymap(lambda element: self.dirty_string_to_int(element))

        # convert to unique pandas serie
        df = self.unstack_dataframe_to_serie(df)
        df = df[df["year"] <= self.max_year]

        # TODO - ajouter la conversion en anglais ?
        return df

