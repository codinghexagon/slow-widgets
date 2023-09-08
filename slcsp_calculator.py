import time
import argparse
import pandas as pd

class SLCSPCalculator:
    def __init__(self, plans_file, zips_file, slcsp_file):
        """
        Initialize the SLCSPCalculator class by loading the data files.
        
        Args:
            plans_file: The path to the CSV file containing the plan data.
            zips_file: The path to the CSV file containing the ZIP code data.
            slcsp_file: The path to the CSV file containing the SLCSP data.
        """
        self.plans_data = pd.read_csv(plans_file)
        self.zipcode_data = pd.read_csv(zips_file)
        self.slcsp_data = pd.read_csv(slcsp_file)

    def calculate_slcsp(self):
        """
        Main method to calculate the Second Lowest Cost Silver Plan (SLCSP).
        It sequentially calls the methods that perform each step of the calculation.
        """
        self.filter_silver_plans()
        self.merge_data()
        self.calculate_second_lowest_rates()
        self.merge_zip_and_rate()
        self.determine_rate()

    def filter_silver_plans(self):
        """
        Filters the plans data to only include rows where the metal level is 'Silver'.
        """
        self.silver_plans = self.plans_data[self.plans_data['metal_level'] == 'Silver']

    def merge_data(self):
        """
        Merges the ZIP code data with the Silver plan data based on 'state' and 'rate_area' columns.
        """
        self.merged_data = pd.merge(self.zipcode_data, self.silver_plans, on=['state', 'rate_area'])

    def calculate_second_lowest_rates(self):
        """
        Calculates the second lowest unique rate for each state-rate_area pair.
        """
        self.second_lowest_rates = (self.merged_data.groupby(['state', 'rate_area'])
                                    .apply(self.get_second_lowest_rate)
                                    .reset_index().rename(columns={0: 'second_lowest_rate'}))

    def get_second_lowest_rate(self, df):
        """
        Helper method to calculate the second lowest unique rate in a given dataframe.

        Args:
            df: DataFrame containing 'rate' column.
        
        Returns:
            Second lowest unique rate if there are at least 2 unique rates, else None.
        """
        unique_rates = df['rate'].drop_duplicates()
        if unique_rates.size > 1:
            return unique_rates.nsmallest(2).values[1]
        return None

    def merge_zip_and_rate(self):
        """
        Merges the second lowest rates back into the ZIP code data.
        This adds a new column 'second_lowest_rate' to the ZIP code data.
        """
        self.zipcode_data = pd.merge(self.zipcode_data, self.second_lowest_rates, on=['state', 'rate_area'], how='left')

    def determine_rate(self):
        """
        Determines the SLCSP for each ZIP code in the SLCSP data.
        For ZIP codes associated with more than one rate area or where there isn't a second lowest rate, the rate is set as ''.
        """
        self.slcsp_data['rate'] = self.slcsp_data['zipcode'].apply(self.get_rate)
        self.slcsp_data['rate'] = self.slcsp_data['rate'].apply(lambda rate: f"{rate:.2f}" if rate != '' else rate)

    def get_rate(self, zipcode):
        """
        Helper method to get the SLCSP for a given ZIP code.
        
        Args:
            zipcode: ZIP code for which to get the SLCSP.
        
        Returns:
            SLCSP if it exists, else ''.
        """
        zipcode_data = self.zipcode_data[self.zipcode_data['zipcode'] == zipcode]
        if zipcode_data['rate_area'].nunique() > 1 or zipcode_data['second_lowest_rate'].isna().any():
            return ''
        return zipcode_data['second_lowest_rate'].values[0]

    def print_slcsp(self):
        """
        Prints the SLCSP data in CSV format to the console.
        """
        print(self.slcsp_data.to_csv(index=False))

def main():
    """
    Main function to parse command line arguments and use the SLCSPCalculator class.
    """
    parser = argparse.ArgumentParser(description="Calculate Second Lowest Cost Silver Plan (SLCSP)")
    parser.add_argument("--plans", required=True, help="Path to plans CSV file")
    parser.add_argument("--zips", required=True, help="Path to zips CSV file")
    parser.add_argument("--slcsp", required=True, help="Path to slcsp CSV file")
    args = parser.parse_args()

    start_time = time.time()

    # Create a SLCSPCalculator object and use it to calculate and print the SLCSP.
    slcsp_calculator = SLCSPCalculator(args.plans, args.zips, args.slcsp)
    slcsp_calculator.calculate_slcsp()
    slcsp_calculator.print_slcsp()

    end_time = time.time()

    print(f"\nProgram Execution Time: {end_time - start_time} seconds")

if __name__ == "__main__":
    main()
