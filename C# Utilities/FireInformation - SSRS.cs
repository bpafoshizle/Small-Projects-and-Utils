
    private void simpleInformationFire(string message)
    {
        //C# code  
        bool pbCancel = false;
        this.ComponentMetaData.FireInformation(0, "myScriptComponent", message, "", 0, ref pbCancel);
    }