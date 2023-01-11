const fetch = require("node-fetch");

const checkAuth = (req) => {
  const token = req.headers.authorization.split(" ")[1];
  if (token !== process.env.WELD_API_KEY) {
    const err = new Error("Not authorized");
    err.status = 403;
    throw err;
  }
};

const url = "https://sheets.googleapis.com/v4/spreadsheets/";

// sheets that can be requested in this integration and their range
// read more about ranges here: https://developers.google.com/sheets/api/samples/reading
const sheets = {
  sheet1: {
    id: "11Uhjl9xQEQk6l7ZFfhFzsvPs6sFgG65JovPM4DZ5nCg",
    range: "TestNamedRange",
  },
};
const api_key = process.env.GOOGLE_SHEETS_API_KEY;

exports.handler = async (req, res) => {
  // make sure the request is coming from Weld
  checkAuth(req);

  // if schema is requested, return schema
  if (req.originalUrl === "/schema") return res.json(getSchema());

  // fetch the data from Google Sheets API
  const sheet = sheets[req.body.name];
  const response = await fetch(
    url + sheet.id + "/values/" + sheet.range + "?key=" + api_key
  );
  const json = await response.json();
  if (json.error) throw new Error(json.error.message);
  const [keys, ...rows] = json.values;

  res.json({
    insert: rows.map((row) => {
      return Object.values(keys).reduce((column, key, index) => {
        return { ...column, [key]: row[index] };
      }, {});
    }),
    state: {},
    hasMore: false,
  });
};

const getSchema = () => {
  return {
    schema: {
      sheet1: {
        primary_key: "id",
      },
    },
  };
};
