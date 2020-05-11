using System;

namespace MDBW2020AttributeVsWildcard {
  class Program {

    static void Main(string[] args) {
      if (args.Length == 0) {
        Console.WriteLine("Please enter a numeric argument.");
        return;
      }

      switch (args[0].ToLower()) {
        case "dataloader":
          new DataLoader().Main();
          break;

        case "equalitybenchmark":
          new EqualityBenchmark().Main();
          break;

        default:
          Console.WriteLine(string.Format("Invalid Mode {0}.",args[0]));
          break;
      }

    }
  }
}
