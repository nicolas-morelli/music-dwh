import os
import sys
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dags')))

from apifunctions.api import process_artist, process_tracks, process_albums, process_track, process_album


class TestLastFmProcessing(unittest.TestCase):

    def setUp(self):
        self.key = 'some_key'
        self.artist_data = {'artist': {'name': 'Kublai Khan TX', 'stats': {'listeners': '5000000', 'playcount': '100000000'}}}
        self.track_data = {'toptracks': {'track': [{'name': 'The Hammer', 'playcount': '5000000', 'listeners': '1000000', '@attr': {'rank': '1'}}, {'name': 'No Kin', 'playcount': '4000000', 'listeners': '800000', '@attr': {'rank': '2'}}]}}
        self.album_data = {'topalbums': {'album': [{'name': 'Absolute', 'playcount': '3000000'}, {'name': 'Nomad', 'playcount': '2500000'}]}}
        self.tagartists = pd.DataFrame([{'name': 'Kublai Khan TX', 'rank': 1, 'a field': 'a result'}, {'name': 'Knocked Loose', 'rank': 2, 'a field': 'a result'}])

    @patch('requests.get')
    def test_process_artist(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = self.artist_data
        mock_get.return_value = mock_response

        result = process_artist('Metalcore', 'Kublai Khan TX', self.key, self.tagartists, 0)

        expected = {'name': 'Kublai Khan TX', 'tag': 'Metalcore', 'listeners': '5000000', 'playcount': '100000000', 'rank': 1, 'stats_date': datetime.now().strftime('%Y-%m-%d')}

        self.assertEqual(result, expected)

    @patch('requests.get')
    def test_process_tracks(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = self.track_data
        mock_get.return_value = mock_response

        result = process_tracks('Kublai Khan TX', self.key, self.tagartists, 0)

        expected = [{'name': 'The Hammer', 'playcount': '5000000', 'listeners': '1000000', 'rank': 1, 'stats_date': datetime.now().strftime('%Y-%m-%d'), 'artist': 'Kublai Khan TX'},
                    {'name': 'No Kin', 'playcount': '4000000', 'listeners': '800000', 'rank': 2, 'stats_date': datetime.now().strftime('%Y-%m-%d'), 'artist': 'Kublai Khan TX'}]

        self.assertEqual(result, expected)

    @patch('requests.get')
    def test_process_albums(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = self.album_data
        mock_get.return_value = mock_response

        result = process_albums('Kublai Khan TX', self.key, self.tagartists, 0)

        expected = [{'name': 'Absolute', 'playcount': '3000000', 'stats_date': datetime.now().strftime('%Y-%m-%d'), 'artist': 'Kublai Khan TX'},
                    {'name': 'Nomad', 'playcount': '2500000', 'stats_date': datetime.now().strftime('%Y-%m-%d'), 'artist': 'Kublai Khan TX'}]

        self.assertEqual(result, expected)

    def test_process_track(self):
        result = process_track(0, MagicMock(json=MagicMock(return_value=self.track_data)), 'Kublai Khan TX')

        expected = {'name': 'The Hammer', 'playcount': '5000000', 'listeners': '1000000', 'rank': 1, 'stats_date': datetime.now().strftime('%Y-%m-%d'), 'artist': 'Kublai Khan TX'}

        self.assertEqual(result, expected)

    def test_process_album(self):
        result = process_album(0, MagicMock(json=MagicMock(return_value=self.album_data)), 'Kublai Khan TX')

        expected = {'name': 'Absolute', 'playcount': '3000000', 'stats_date': datetime.now().strftime('%Y-%m-%d'), 'artist': 'Kublai Khan TX'}

        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
