from __future__ import annotations

import copy
import json
import unittest

from google_saved_lists.parser import ParseError, parse_saved_list_artifacts

_LIST_URL = (
    "https://www.google.com/maps/@35.6501307,139.6868459,15z/"
    "data=!4m3!11m2!2sUGEPbA20Qd-OH4uoWjmDgQ!3e3"
)
_SHORT_URL = "https://maps.app.goo.gl/MG2Vd5pWBkL7hXL18"
_REDIRECT_URL = (
    "https://www.google.com/maps/@30.5370705,125.4120472,6z/"
    "data=!4m3!11m2!2sUGEPbA20Qd-OH4uoWjmDgQ!3e3?entry=ttu"
)
_LIST_NODE = [
    ["UGEPbA20Qd-OH4uoWjmDgQ", 1, None, 1, 1],
    4,
    "https://www.google.com/maps/placelists/list/UGEPbA20Qd-OH4uoWjmDgQ",
    "Owner",
    "Tokyo Dinners",
    "Best spots in the city",
    None,
    None,
    [
        [
            None,
            [
                None,
                None,
                "",
                None,
                "Shibuya, Tokyo",
                [None, None, 35.6501307, 139.6868459],
                ["7451636382641713350", "aux"],
                "/g/11yakumo",
            ],
            "Yakumo",
            "Delicious wonton ramen. You can ask for a mix of white and dark broth.",
            None,
            None,
            None,
            [[[[3, None, "104356373423434804635", "❤️", [1776133481, 81561000]]]]],
        ],
        [
            None,
            [
                None,
                None,
                "",
                None,
                "Chuo City, Tokyo",
                [None, None, 35.6915776, 139.7836109],
                ["1234567890123456789"],
                "/g/11sushi",
            ],
            "Sushi Place",
        ],
    ],
]


class ParserTests(unittest.TestCase):
    def test_parses_runtime_state_with_list_id(self) -> None:
        runtime_state = ["noise", _LIST_NODE]

        parsed = parse_saved_list_artifacts(_LIST_URL, runtime_state=runtime_state)

        self.assertEqual(parsed.list_id, "UGEPbA20Qd-OH4uoWjmDgQ")
        self.assertEqual(parsed.title, "Tokyo Dinners")
        self.assertEqual(parsed.description, "Best spots in the city")
        self.assertEqual(len(parsed.places), 2)
        self.assertEqual(parsed.places[0].name, "Yakumo")
        self.assertEqual(
            parsed.places[0].note,
            "Delicious wonton ramen. You can ask for a mix of white and dark broth.",
        )
        self.assertTrue(parsed.places[0].is_favorite)
        self.assertFalse(parsed.places[1].is_favorite)
        self.assertEqual(parsed.places[0].cid, "7451636382641713350")
        self.assertEqual(parsed.places[0].maps_url, "https://maps.google.com/?cid=7451636382641713350")

    def test_falls_back_to_placelist_marker_without_list_id(self) -> None:
        runtime_state = ["noise", _LIST_NODE]

        parsed = parse_saved_list_artifacts(
            "https://www.google.com/maps",
            runtime_state=runtime_state,
        )

        self.assertEqual(parsed.list_id, "UGEPbA20Qd-OH4uoWjmDgQ")
        self.assertEqual(parsed.title, "Tokyo Dinners")
        self.assertEqual(parsed.places[1].name, "Sushi Place")
        self.assertFalse(parsed.places[1].is_favorite)

    def test_prefers_list_id_from_resolved_redirect_url(self) -> None:
        runtime_state = ["noise", _LIST_NODE]

        parsed = parse_saved_list_artifacts(
            _SHORT_URL,
            resolved_url=_REDIRECT_URL,
            runtime_state=runtime_state,
        )

        self.assertEqual(parsed.source_url, _SHORT_URL)
        self.assertEqual(parsed.resolved_url, _REDIRECT_URL)
        self.assertEqual(parsed.list_id, "UGEPbA20Qd-OH4uoWjmDgQ")
        self.assertEqual(parsed.title, "Tokyo Dinners")

    def test_decodes_embedded_xssi_blob(self) -> None:
        blob = ")]}'\\n" + json.dumps(_LIST_NODE)

        parsed = parse_saved_list_artifacts(_LIST_URL, script_texts=[blob])

        self.assertEqual(parsed.resolved_url, None)
        self.assertEqual(parsed.title, "Tokyo Dinners")
        self.assertEqual(len(parsed.places), 2)

    def test_decodes_app_initialization_state_assignment(self) -> None:
        blob = f"window.APP_INITIALIZATION_STATE={json.dumps(_LIST_NODE)};"

        parsed = parse_saved_list_artifacts(_LIST_URL, script_texts=[blob])

        self.assertEqual(parsed.list_id, "UGEPbA20Qd-OH4uoWjmDgQ")
        self.assertEqual(parsed.title, "Tokyo Dinners")

    def test_builds_coordinate_query_url_when_cid_is_missing(self) -> None:
        runtime_state = copy.deepcopy(["noise", _LIST_NODE])
        place_metadata = runtime_state[1][8][0][1]
        assert isinstance(place_metadata, list)
        place_metadata[6] = [None]

        parsed = parse_saved_list_artifacts(_LIST_URL, runtime_state=runtime_state)

        self.assertEqual(parsed.places[0].cid, None)
        self.assertEqual(parsed.places[0].google_id, "/g/11yakumo")
        self.assertEqual(
            parsed.places[0].maps_url,
            "https://maps.google.com/?q=35.6501307,139.6868459",
        )

    def test_dedupes_places_with_same_cid(self) -> None:
        runtime_state = copy.deepcopy(["noise", _LIST_NODE])
        duplicate_place = copy.deepcopy(runtime_state[1][8][0])
        runtime_state[1][8].append(duplicate_place)

        parsed = parse_saved_list_artifacts(_LIST_URL, runtime_state=runtime_state)

        self.assertEqual(len(parsed.places), 2)
        self.assertEqual(parsed.places[0].cid, "7451636382641713350")
        self.assertEqual(parsed.places[1].cid, "1234567890123456789")

    def test_extracts_favorite_and_note_from_user_payload_shape(self) -> None:
        runtime_state = [
            "noise",
            [
                ["UGEPbA20Qd-OH4uoWjmDgQ", 1, None, 1, 1],
                4,
                "https://www.google.com/maps/placelists/list/UGEPbA20Qd-OH4uoWjmDgQ",
                "Owner",
                "Tokyo Dinners",
                "Best spots in the city",
                None,
                None,
                [
                    [
                        None,
                        [
                            None,
                            None,
                            "",
                            None,
                            (
                                "Japan, 〒153-0051 Tokyo, Meguro City, Kamimeguro, "
                                "2 Chome−12−2 W.nakameguro 1F"
                            ),
                            [None, None, 35.6426886, 139.6988208],
                            ["6924437575605096209", "-782808945063765017"],
                            "/g/1pty5xgj1",
                        ],
                        "MARU",
                        "Foie gras, caviar and truffle oyakodon!",
                        None,
                        None,
                        None,
                        [[[[3, None, "104356373423434804635", "❤️", [1776133481, 81561000]]]]],
                        [[1], ["6924437575605096209", "-782808945063765017"]],
                        [1776063335, 302383000],
                        [1776132745, 850748000],
                        None,
                    ]
                ],
            ],
        ]

        parsed = parse_saved_list_artifacts(_LIST_URL, runtime_state=runtime_state)

        self.assertEqual(len(parsed.places), 1)
        self.assertEqual(parsed.places[0].name, "MARU")
        self.assertEqual(
            parsed.places[0].note,
            "Foie gras, caviar and truffle oyakodon!",
        )
        self.assertTrue(parsed.places[0].is_favorite)

    def test_extracts_favorite_when_place_name_is_missing(self) -> None:
        runtime_state = [
            "noise",
            [
                ["UGEPbA20Qd-OH4uoWjmDgQ", 1, None, 1, 1],
                4,
                "https://www.google.com/maps/placelists/list/UGEPbA20Qd-OH4uoWjmDgQ",
                "Owner",
                "Tokyo Dinners",
                "Best spots in the city",
                None,
                None,
                [
                    [
                        None,
                        [
                            None,
                            None,
                            "",
                            None,
                            "Nakameguro, Tokyo",
                            [None, None, 35.6426886, 139.6988208],
                            ["6924437575605096209", "-782808945063765017"],
                            "/g/1pty5xgj1",
                        ],
                        None,
                        None,
                        None,
                        None,
                        None,
                        [[[[3, None, "104356373423434804635", "❤️", [1776133481, 81561000]]]]],
                    ]
                ],
            ],
        ]

        parsed = parse_saved_list_artifacts(_LIST_URL, runtime_state=runtime_state)

        self.assertEqual(len(parsed.places), 1)
        self.assertEqual(parsed.places[0].address, "Nakameguro, Tokyo")
        self.assertTrue(parsed.places[0].is_favorite)

    def test_raises_when_no_place_records_are_found(self) -> None:
        runtime_state = copy.deepcopy(["noise", _LIST_NODE])
        runtime_state[1][8] = []

        with self.assertRaises(ParseError):
            parse_saved_list_artifacts(_LIST_URL, runtime_state=runtime_state)


if __name__ == "__main__":
    unittest.main()
