const MongoClient = require('mongodb').MongoClient;

// Connection URL
const url = 'mongodb://localhost:27017';

// Database Name
const dbName = 'study';

// Use connect method to connect to the server
MongoClient.connect(url, {useUnifiedTopology: true}, async function (err, client) {
  const db = client.db(dbName);
  const students = db.collection('students');

  const showResult = (array) => {
    array.forEach((value) => {
      console.log(value)
    })
  };

//  1. Find all students who have the worst score for homework, sort by descent
  let sortByDescent = await students.find({
    'scores.2.type': 'homework',
    'scores.2.score': {$lt: 20}
  }).sort({"scores.2": -1});
  showResult(sortByDescent);

//  2. Find all students who have the best score for quiz and the worst for homework, sort by ascending
  let sortByAscend = await students.find({'scores.1.score': {$gt: 50}, 'scores.2.score': {$lt: 20}}).sort({
    "scores.1": 1,
    "scores.2": 1
  });
  showResult(sortByAscend);

//  3. Find all students who have best scope for quiz and exam
  let bestByTwoParams = await students.find({'scores.0.score': {$gt: 90}, 'scores.1.score': {$gt: 90}});
  showResult(bestByTwoParams);

// 4. Calculate the average score for homework for all students
  let average = await students.aggregate(
    [
      {
        $project: {
          scores: {
            $filter: {
              input: "$scores",
              as: "item",
              cond: {$eq: ["$$item.type", "homework"]}
            }
          }
        }
      },
      {
        $project:
          {
            homework_score: {$arrayElemAt: ["$scores", 0]}
          }
      },
      {
        $group:
          {
            _id: "homework",
            average_score: {$avg: "$homework_score.score"}
          }
      }
    ]
  );
  showResult(average);

//  5. Delete all students that have homework score <= 60
//   let deleteStudents = await students.deleteMany({"scores.2.score": {$lte: 60}});
//   showResult(deleteStudents);

//  6. Mark students that have quiz score => 80
  let markStudents = await students.updateMany({"scores.1.score": {$gte: 80}}, {$set: {"Has quiz score => 80": true}});
  showResult(markStudents);

//  7. Write a query that group students by 3 categories (calculate the average grade for three subjects)
//   - a => (between 0 and 40)
//   - b => (between 40 and 60)
//   - c => (between 60 and 100)

  let groupStudents = await students.aggregate([
    {
      $project:
        {
          name: 1,
          exam_score: {$arrayElemAt: ["$scores", 0]},
          quiz_score: {$arrayElemAt: ["$scores", 1]},
          homework_score: {$arrayElemAt: ["$scores", 2]}
        }
    },
    {
      $facet: {

        "exam_group": [
          {
            $bucket: {
              groupBy: "$exam_score.score",
              boundaries: [0, 40, 60, 100],
              default: "Other",
              output: {
                "average_exam_score": {$avg: "$exam_score.score"},
                "students": {$push: {"name": "$name", "exam_score": "$exam_score.score"}}
              }
            }
          }
        ],

        "quiz_group": [
          {
            $bucket: {
              groupBy: "$quiz_score.score",
              boundaries: [0, 40, 60, 100],
              default: "Other",
              output: {
                "average_quiz_score": {$avg: "$quiz_score.score"},
                "students": {$push: {"name": "$name", "quiz_score": "$quiz_score.score"}}
              }
            }
          }
        ],

        "homework_group": [
          {
            $bucket: {
              groupBy: "$homework_score.score",
              boundaries: [0, 40, 60, 100],
              default: "Other",
              output: {
                "average_homework_score": {$avg: "$homework_score.score"},
                "students": {$push: {"name": "$name", "homework_score": "$homework_score.score"}}
              }
            }
          }
        ]
      }
    }
  ]);
  showResult(groupStudents);

  client.close();
});
