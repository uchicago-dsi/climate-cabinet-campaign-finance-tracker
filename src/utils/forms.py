import abc

class Form(abc.ABC):

    def open_form(self, path: str) -> pd.DataFrame:
        """open form into a dataframe"""
        pass

    def map_columns(self):
        return self.table.rename(columns=self.column_mapper)


class TexasContributionForm(Form):


    column_mapper = {
        "contributionAmount": "Amount"
    }

    def convert_date_format()


class TexasExpenseForm(Form):

    column_mapper = {
         
    }