import requests
from bs4 import BeautifulSoup
import pandas as pd
import argparse
from tqdm import tqdm

# URL of the site to scrape

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A script for scraping agricultural data from the USDA website."
    )

    # Adding the arguments:

    # Start year argument
    parser.add_argument(
        "--start_year",
        type=int,
        default=2002,
        help="The start year for the data scraping (e.g., 2002).",
    )

    # End year argument
    parser.add_argument(
        "--end_year",
        type=int,
        default=2023,
        help="The end year for the data scraping (e.g., 2023).",
    )

    # States argument (accepts multiple values)
    parser.add_argument(
        "--states",
        nargs="+",  # Allows multiple values to be passed
        default=["UTAH"],
        help="List of states to scrape data for (e.g., UTAH, TEXAS).",
    )

    # Output path argument
    parser.add_argument(
        "--output_path",
        type=str,
        default=".",
        help="Directory where the scraped data will be saved (default is the current directory).",
    )

    # Parse the arguments
    args = parser.parse_args()

    url = "https://www.nass.usda.gov/Quick_Stats/Ag_Overview/stateOverview.php?"
    dfs = []
    for state in args.states:
        print(f"===== {state} ======")
        for year in tqdm(range(args.start_year, args.end_year + 1)):
            # Send a GET request to the website
            response = requests.get(f"{url}state={state.upper()}&year={str(year)}")

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Parse the HTML content of the page with BeautifulSoup
                soup = BeautifulSoup(response.content, "html.parser")

                # Find the table by looking for the first table with the desired headers
                table = soup.find(
                    "table", {"border": "1"}
                )  # This is the table you are interested in

                # List to store extracted data
                data = []

                # Iterate over rows in the table
                for row in table.find_all("tr")[1:]:  # Skip the header row
                    cells = row.find_all("td")
                    # Extract text from each cell and clean it
                    if len(cells) > 1:
                        commodity_data = {
                            "Commodity": cells[0].get_text(strip=True),
                            "Planted All Purpose Acres": cells[1].get_text(strip=True),
                            "Harvested Acres": cells[2].get_text(strip=True),
                            "Yield": cells[3].get_text(strip=True),
                            "Production": cells[4].get_text(strip=True),
                            "Price per Unit": cells[5].get_text(strip=True),
                            "Value of Production in Dollars": cells[6].get_text(
                                strip=True
                            ),
                        }
                        data.append(commodity_data)

                df = pd.DataFrame(data)
                df = df[df["Value of Production in Dollars"].str.strip() != ""]
                df["Unit"] = df["Price per Unit"].apply(
                    lambda x: x.split("/")[-1].strip()
                )
                df["Amount Produced"] = df["Production"].apply(
                    lambda x: x.split(" ")[0].strip()
                )
                df["Year"] = year
                df["State"] = state.lower()
                dfs.append(df)
            else:
                print(
                    f"Failed to retrieve the webpage. Status code: {response.status_code}"
                )
    df = pd.concat(dfs)
    df.to_csv(
        f"{args.output_path}/agg_revenue_{start_year}-{end_year}.csv", index=False
    )
