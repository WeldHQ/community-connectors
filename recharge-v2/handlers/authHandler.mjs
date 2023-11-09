//2.2 Weld API (set key in cloud function env variable)
export const checkAuth = (req) => {
    const token = req.headers.authorization.split(" ")[1];
    if (token !== process.env.WELD_API_KEY) {
      const err = new Error("Not authorized");
      err.status = 403;
      throw err;
    }
  };