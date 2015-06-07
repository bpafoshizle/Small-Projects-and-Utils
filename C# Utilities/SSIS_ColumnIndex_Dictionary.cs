// TESTER:   
   public override void PreExecute()
    {
        base.PreExecute();
        /*
          Add your code here for preprocessing or remove if not needed
        */
        var input = this.ComponentMetaData.InputCollection[0];
        var indexes = this.GetColumnIndexes(input.ID);
        int offset = 0;
      
        foreach(IDTSInputColumn100 col in input.InputColumnCollection)
        {            
            this.Log(String.Format("lineageID {0} Name {1} Buffer Index {2} offset {3}", col.LineageID, col.Name, indexes[offset], offset), 0, null);
            offset++;
        }
    }
// IMPLEMENTED:
private Dictionary<string, int> buildColumnDictionary()
    {
        var input = this.ComponentMetaData.InputCollection[0];
        var indexes = this.GetColumnIndexes(input.ID);
        int offset = 0;
        Dictionary<string, int> nameToIndexMap = new Dictionary<string,int>();

        foreach(IDTSInputColumn100 col in input.InputColumnCollection)
        {
            nameToIndexMap.Add(col.Name, indexes[offset]);
            //this.Log(String.Format("lineageID {0} Name {1} Buffer Index {2} offset {3}", col.LineageID, col.Name, indexes[offset], offset), 0, null);
            offset++;
        }

        return nameToIndexMap;
    }

// USE LIKE:
Buffer.SetString(nameToIndexMap["HAS_DATA"], "Y");
Buffer.SetString(nameToIndexMap["SHIPTO_LUD"], String.Format("{0:yyyy'-'MM'-'dd HH':'mm':'ss}", columnLUD));