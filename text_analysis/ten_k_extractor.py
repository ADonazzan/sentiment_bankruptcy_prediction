import logging
import re
import pandas as pd
from bs4 import BeautifulSoup

from text_analysis.sec_scraper import SECScraper


class TenKExtractor:
    """
    A class to extract and process 10-K filings for a given company.

    Attributes:
    ----------
    cik_code : str
        The CIK code of the company.
    year_start : str
        The start year for extracting 10-K filings.
    year_end : str
        The end year for extracting 10-K filings.
    """

    def __init__(self, cik_code, year_start, year_end):
        """
        Constructs all the necessary attributes for the TenKExtractor object.

        Parameters:
        ----------
        cik_code : str
            The CIK code of the company.
        year_start : str
            The start year for extracting 10-K filings.
        year_end : str
            The end year for extracting 10-K filings.
        """
        self.cik_code = cik_code
        self.year_start = year_start
        self.year_end = year_end

    def clean_ten_k(self, raw_document):
        """
        Cleans the 10-K filing by extracting only the 10-K section.

        Parameters:
        ----------
        raw_document : str
            The raw 10-K filing.

        Returns:
        -------
        dict
            A dictionary containing the cleaned 10-K section.
        """
        # Regex to find <DOCUMENT> tags
        doc_start_pattern = re.compile(r'<DOCUMENT>')
        doc_end_pattern = re.compile(r'</DOCUMENT>')
        # Regex to find <TYPE> tag prceeding any characters, terminating at new line
        type_pattern = re.compile(r'<TYPE>[^\n]+')

        # There are many <Document> Tags in this text file, each as specific exhibit like 10-K, EX-10.17 etc
        # First filter will give us document tag start <end> and document tag end's <start>
        # We will use this to later grab content in between these tags
        doc_start_is = [x.end() for x in doc_start_pattern.finditer(raw_document)]
        doc_end_is = [x.start() for x in doc_end_pattern.finditer(raw_document)]

        # Create a loop to go through each section type and store the type and content in the dictionary
        doc_types = [x[len('<TYPE>'):] for x in type_pattern.findall(raw_document)]

        document = {}
        # Create a loop to go through each section type and save only the 10-K section in the dictionary
        for doc_type, doc_start, doc_end in zip(doc_types, doc_start_is, doc_end_is):
            if doc_type == '10-K':
                document[doc_type] = raw_document[doc_start:doc_end]

        return document

    def get_section_boundaries(self, raw_document):
        """
        Finds the boundaries of sections in the 10-K filing.

        Parameters:
        ----------
        raw_document : dict
            The cleaned 10-K filing.

        Returns:
        -------
        pd.DataFrame
            A DataFrame containing the boundaries of the sections.
        """
        # Regex to find the sections
        regex = re.compile(r'(?i)\bitem(?:\s|&#160;|&nbsp;)*?(1A|1|7A|7|9A)\b')
        matches = regex.finditer(raw_document['10-K'])

        # Create the dataframe
        sections = pd.DataFrame([(x.group(), x.start(), x.end()) for x in matches])

        sections.columns = ['item', 'start', 'end']
        sections['item'] = sections.item.str.lower()

        # Get rid of unnesesary charcters from the dataframe
        sections.replace('&#160;', ' ', regex=True, inplace=True)
        sections.replace('&nbsp;', ' ', regex=True, inplace=True)
        sections.replace(' ', '', regex=True, inplace=True)
        sections.replace('\.', '', regex=True, inplace=True)
        sections.replace('>', '', regex=True, inplace=True)

        # Drop duplicates
        sections = sections.sort_values('start', ascending=True).drop_duplicates(subset=['item'], keep='last')

        return sections

    def parse_sections(self, raw_document):
        """
        Parses the sections of the 10-K filing.

        Parameters:
        ----------
        raw_document : dict
            The cleaned 10-K filing.

        Returns:
        -------
        dict
            A dictionary containing the parsed sections of the 10-K filing.
        """
        section_boundaries = self.get_section_boundaries(raw_document)
        document = raw_document['10-K']
        parsed_sections = {}
        for i in range(len(section_boundaries)):
            if i + 1 < len(section_boundaries):
                section_text = document[section_boundaries.iloc[i]['end']:section_boundaries.iloc[i + 1]['start']]
            else:
                section_text = document[section_boundaries.iloc[i]['end']:]
            # Clean the section text
            section_text = BeautifulSoup(section_text, 'lxml').get_text("\n").replace('\xa0', ' ')
            parsed_sections[section_boundaries.iloc[i]['item']] = section_text

        return parsed_sections

    def get_ten_k_filings(self):
        """
        Extracts and polishes all 10-K filings for a given company within a given date range.

        Returns:
        -------
        dict
            A dictionary containing sections 1, 1A, 7, 7A, and 9A of all 10-K filings.
        """
        scraper = SECScraper()
        submissions = scraper.get_10_k_descriptions(cik_code=self.cik_code, date_start=f"{self.year_start}-01-01",
                                                    date_end=f"{self.year_end}-12-31")
        ten_k_filings = {}
        for submission in submissions:
            ten_k_filing = scraper.download_10k(cik_code=self.cik_code, accession_number=submission['accessionNumber'])
            cleaned_ten_k = self.clean_ten_k(ten_k_filing)
            parsed_sections = self.parse_sections(cleaned_ten_k)
            ten_k_filings[submission['filingDate']] = parsed_sections

        logging.info(f"Extracted {len(ten_k_filings)} 10-K filings for company {self.cik_code} "
                     f"between {self.year_start} and {self.year_end}")
        # Dimension of one 10-K filing: 2.1 MB
        return ten_k_filings


if __name__ == '__main__':
    cik_code = "320193"
    date_range = ('2020', '2020')
    extractor = TenKExtractor(cik_code, date_range[0], date_range[1])
    print(extractor.get_ten_k_filings())
