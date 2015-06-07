using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.IO;

namespace Tyson.BI.IS.Cognos.SDK.CognosStatistics
{
    class CogObject
    {
        private string _id;
        private string _name;
        private List<CogObject> _subElements = new List<CogObject>();
        private Dictionary<string, string> _attributeList = new Dictionary<string, string>();
        
        public string ID
        {
            get { return _id; }
            set { _id = value; }
        }
        
        public string Name
        {
            get { return _name; }
            set { _name = value; }
        }
        
        public List<CogObject> SubElements
        {
            get { return _subElements; }
        }

        
        public Dictionary<string, string> AttributeList
        {
            get { return _attributeList; }
        }

        public void AddAttribute(string attrType, string attrValue)
        {
            _attributeList.Add(attrType, attrValue);
        }

        public List<string> getAttributeTypes()
        {
            List<string> atts = new List<string>();
            foreach (string att in _attributeList.Keys)
            {
                atts.Add(att);
            }
            return atts;
        }

        public string getAttributeValue(string key)
        {
            if (_attributeList.ContainsKey(key))
            {
                return _attributeList[key];
            }
            else
            {
                return "null";
            }
        }

        /**
         * Method to append a list of sub elemental CogObjects to the
         * current list.
         */
        public void addSubElements(List<CogObject> cogs)
        {
            _subElements.AddRange(cogs);
        }

        /**
         * Method to append a single CogObject 
         * to the end of the _subElements list
         */
        public void addSubElement(CogObject cog)
        {
            _subElements.Add(cog);
        }

        /**
         * Method that returns an xml spec for the CogObject and all atrributes
         */
        public string returnXML()
        {
            string xmlString;

            StringBuilder output = new StringBuilder();
            StringWriter stringWriter = new StringWriter(output);
            XmlTextWriter xmlWriter = new XmlTextWriter(stringWriter);
            xmlWriter.Formatting = Formatting.Indented;

            //Write start element
            xmlWriter.WriteStartElement("CogObject");

            //List<AttributeTypeEnum> attributes = getAttributeTypes();

            foreach (KeyValuePair<string, string> pair in _attributeList)
            {
                // Write sub elements
                xmlWriter.WriteElementString(pair.Key.ToString(), pair.Value.ToString());
            }

            // End root ReportObject element
            xmlWriter.WriteFullEndElement();

            // Close the xmlWriter
            xmlWriter.Close();

            xmlString = output.ToString();
            return xmlString;
        }
    }
}
