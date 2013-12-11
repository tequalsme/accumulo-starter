package com.timreardon.accumulo.starter.ingest.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import com.timreardon.accumulo.starter.common.domain.Message;

public class MessageParserTest {
    private static final String DATA_FILE_1 = "samples/maildir/allen-p/_sent_mail/1.";
    private static final String DATA_FILE_2 = "samples/maildir/mcconnell-m/all_documents/520.";
    private static final String DATA_FILE_3 = "samples/maildir/skilling-j/inbox/1.";
    private static final String DATA_FILE_4 = "samples/maildir/skilling-j/inbox/genie/1.";
    private static final String DATA_FILE_5 = "samples/maildir/skilling-j/sent/83.";
    private static final String DATA_FILE_6 = "samples/maildir/slinger-r/inbox/28.";

    private MessageParser parser = new MessageParser();
    
    private Message parseData(String path) throws IOException {
        return parser.parse(IOUtils.toByteArray(new ClassPathResource(path).getInputStream()), path);
    }
    
    @Test
    public void testFile1() throws Exception {
        Message m = parseData(DATA_FILE_1);
        
        assertEquals("18782981.1075855378110", m.getId());
        assertEquals(parser.sdf.parse("Mon, 14 May 2001 16:39:00 -0700 (PDT)").getTime(), m.getTimestamp());
        
        assertEquals("allen-p", m.getMailbox());
        assertEquals("_sent_mail", m.getFolder());
        assertEquals("1.", m.getFileName());
        
        assertEquals("phillip.allen@enron.com", m.getFrom());
        assertEquals("tim.belden@enron.com", m.getToAsString());
        assertNull(m.getSubject());
        assertNull(m.getCc());
        assertNull(m.getBcc());
        
        assertEquals(4, m.getBodyTokens().size());
        assertEquals(508, m.getRawBytes().length);
    }
    
    @Test
    public void testFile2() throws Exception {
        Message m = parseData(DATA_FILE_2);
        
        assertEquals("25560432.1075843912286", m.getId());
        assertEquals(parser.sdf.parse("Wed, 20 Dec 2000 18:02:00 -0800 (PST)").getTime(), m.getTimestamp());
        
        assertEquals("mcconnell-m", m.getMailbox());
        assertEquals("all_documents", m.getFolder());
        assertEquals("520.", m.getFileName());
        
        assertEquals("enron.announcements@enron.com", m.getFrom());
        assertEquals("ena.employees@enron.com", m.getToAsString());
        assertEquals("Re-Alignment", m.getSubject());
        assertEquals("joe.kishkill@enron.com,orlando.gonzalez@enron.com,brett.wiggs@enron.com,remi.collonges@enron.com,jeffrey.shankman@enron.com,mike.mcconnell@enron.com,jeffrey.mcmahon@enron.com,raymond.bowen@enron.com,louise.kitchen@enron.com,philippe.bibi@enron.com,rebecca.mcdonald@enron.com,james.hughes@enron.com,mark.frevert@enron.com,greg.whalley@enron.com,richard.shapiro@enron.com,steven.kean@enron.com,james.steffes@enron.com,ben.glisan@enron.com,mark.koenig@enron.com,rick.buy@enron.com,john.sherriff@enron.com,jeff.skilling@enron.com,kenneth.lay@enron.com,cliff.baxter@enron.com,michael.brown@enron.com,mark.palmer@enron.com",m.getCcAsString());
        assertEquals("joe.kishkill@enron.com,orlando.gonzalez@enron.com,brett.wiggs@enron.com,remi.collonges@enron.com,jeffrey.shankman@enron.com,mike.mcconnell@enron.com,jeffrey.mcmahon@enron.com,raymond.bowen@enron.com,louise.kitchen@enron.com,philippe.bibi@enron.com,rebecca.mcdonald@enron.com,james.hughes@enron.com,mark.frevert@enron.com,greg.whalley@enron.com,richard.shapiro@enron.com,steven.kean@enron.com,james.steffes@enron.com,ben.glisan@enron.com,mark.koenig@enron.com,rick.buy@enron.com,john.sherriff@enron.com,jeff.skilling@enron.com,kenneth.lay@enron.com,cliff.baxter@enron.com,michael.brown@enron.com,mark.palmer@enron.com",m.getBccAsString());
        
        assertEquals(630, m.getBodyTokens().size());
        assertEquals(14907, m.getRawBytes().length);
    }
    
    @Test
    public void testFile3() throws Exception {
        Message m = parseData(DATA_FILE_3);
        
        assertEquals("10617394.1075840149224", m.getId());
        assertEquals(parser.sdf.parse("Wed, 6 Jun 2001 10:46:14 -0700 (PDT)").getTime(), m.getTimestamp());
        
        assertEquals("skilling-j", m.getMailbox());
        assertEquals("inbox", m.getFolder());
        assertEquals("1.", m.getFileName());
        
        assertEquals("dorsey@enron.com", m.getFrom());
        assertEquals("jeremy.blachman@enron.com,a..bibi@enron.com,raymond.bowen@enron.com,london.brown@enron.com,rick.buy@enron.com,richard.causey@enron.com,wade.cline@enron.com,david.cox@enron.com,david.delainey@enron.com,james.derrick@enron.com,m..elliott@enron.com,jim.fallon@enron.com,andrew.fastow@enron.com,mark.frevert@enron.com,ben.glisan@enron.com,kevin.hannon@enron.com,rod.hayslett@enron.com,stanley.horton@enron.com,a..hughes@enron.com,steven.kean@enron.com,louise.kitchen@enron.com,mark.koenig@enron.com,kenneth.lay@enron.com,john.lavorato@enron.com,dan.leff@enron.com,danny.mccarty@enron.com,mike.mcconnell@enron.com,rebecca.mcdonald@enron.com,jeffrey.mcmahon@enron.com,mark.metts@enron.com,mark.muller@enron.com,cindy.olson@enron.com,lou.pai@enron.com,kenneth.rice@enron.com,matthew.scrimshaw@enron.com,a..shankman@enron.com,jeffrey.sherrick@enron.com,john.sherriff@enron.com,jeff.skilling@enron.com,marty.sunde@enron.com,greg.whalley@enron.com,greg.piper@enron.com,janet.dietrich@enron.com",m.getToAsString());
        assertNull(m.getSubject());
        assertEquals("g.g..garcia@enron.com,k..heathman@enron.com,sharron.westbrook@enron.com,kay.chapman@enron.com,j.harris@enron.com,bridget.maronge@enron.com,nicki.daw@enron.com,inez.dauterive@enron.com,ann.brown@enron.com,cindy.stark@enron.com,maureen.mcvicker@enron.com,joannie.williamson@enron.com,rosalee.fleming@enron.com,l..wells@enron.com,cathy.phillips@enron.com,loretta.brelsford@enron.com,sue.ford@enron.com,dolores.fisher@enron.com,karen.owens@enron.com,dorothy.dalton@enron.com,mercedes.estrada@enron.com,christina.grow@enron.com,lauren.urquhart@enron.com,sherri.sera@enron.com,liz.taylor@enron.com,kathy.mcmahon@enron.com,suzanne.danz@enron.com,peggy.mccurley@enron.com,marsha.schiller@enron.com,marisa.rapacioli@enron.com,l..paxton@enron.com,connie.blackwood@enron.com,tammie.schoppe@enron.com,kimberly.hillis@enron.com,jennifer.burns@enron.com,sharon.dick@enron.com,kathy.dodgen@enron.com,kerry.ferrari@enron.com,carol.moffett@enron.com,jennifer.adams@enron.com,leah.rijo@enron.com,lucy.marshall@enron.com,kathy.campos@enron.com,julie.armstrong@enron.com,binky.davidson@enron.com,mrudula.gadade@enron.com,kelly.johnson@enron.com,rebecca.carter@enron.com,tina.spiller@enron.com,vivianna.bolen@enron.com,linda.hawkins@enron.com,vanessa.bob@enron.com,esmeralda.hinojosa@enron.com",m.getCcAsString());
        assertEquals("g.g..garcia@enron.com,k..heathman@enron.com,sharron.westbrook@enron.com,kay.chapman@enron.com,j.harris@enron.com,bridget.maronge@enron.com,nicki.daw@enron.com,inez.dauterive@enron.com,ann.brown@enron.com,cindy.stark@enron.com,maureen.mcvicker@enron.com,joannie.williamson@enron.com,rosalee.fleming@enron.com,l..wells@enron.com,cathy.phillips@enron.com,loretta.brelsford@enron.com,sue.ford@enron.com,dolores.fisher@enron.com,karen.owens@enron.com,dorothy.dalton@enron.com,mercedes.estrada@enron.com,christina.grow@enron.com,lauren.urquhart@enron.com,sherri.sera@enron.com,liz.taylor@enron.com,kathy.mcmahon@enron.com,suzanne.danz@enron.com,peggy.mccurley@enron.com,marsha.schiller@enron.com,marisa.rapacioli@enron.com,l..paxton@enron.com,connie.blackwood@enron.com,tammie.schoppe@enron.com,kimberly.hillis@enron.com,jennifer.burns@enron.com,sharon.dick@enron.com,kathy.dodgen@enron.com,kerry.ferrari@enron.com,carol.moffett@enron.com,jennifer.adams@enron.com,leah.rijo@enron.com,lucy.marshall@enron.com,kathy.campos@enron.com,julie.armstrong@enron.com,binky.davidson@enron.com,mrudula.gadade@enron.com,kelly.johnson@enron.com,rebecca.carter@enron.com,tina.spiller@enron.com,vivianna.bolen@enron.com,linda.hawkins@enron.com,vanessa.bob@enron.com,esmeralda.hinojosa@enron.com",m.getBccAsString());
        
        assertEquals(86, m.getBodyTokens().size());
        assertEquals(12249, m.getRawBytes().length);
    }
    
    @Test
    public void testFile4() throws Exception {
        Message m = parseData(DATA_FILE_4);
        
        assertEquals("793183.1075852686278", m.getId());
        assertEquals(parser.sdf.parse("Wed, 1 Aug 2001 11:26:54 -0700 (PDT)").getTime(), m.getTimestamp());
        
        assertEquals("skilling-j", m.getMailbox());
        assertEquals("inbox", m.getFolder());
        assertEquals("1.", m.getFileName());
        
        assertEquals("a.shreck@occmail.occ.state.ok.us", m.getFrom());
        assertEquals("jeff.skilling@enron.com", m.getToAsString());
        assertEquals("Fall Forum", m.getSubject());
        assertNull(m.getCc());
        assertNull(m.getBcc());
        
        assertEquals(37, m.getBodyTokens().size());
        assertEquals(886, m.getRawBytes().length);
    }
    
    @Test
    public void testFile5() throws Exception {
        Message m = parseData(DATA_FILE_5);
        
        assertEquals("10708849.1075840101852", m.getId());
        assertEquals(parser.sdf.parse("Mon, 28 Aug 2000 08:01:00 -0700 (PDT)").getTime(), m.getTimestamp());
        
        assertEquals("skilling-j", m.getMailbox());
        assertEquals("sent", m.getFolder());
        assertEquals("83.", m.getFileName());
        
        assertEquals("sherri.sera@enron.com", m.getFrom());
        assertEquals("slc1856@sunflower.com", m.getToAsString());
        assertEquals("Re: Thanks for dinner", m.getSubject());
        assertNull(m.getCc());
        assertNull(m.getBcc());
        
        assertEquals(21, m.getBodyTokens().size());
        assertEquals(605, m.getRawBytes().length);
    }
    
    @Test
    public void testFile6() throws Exception {
        Message m = parseData(DATA_FILE_6);
        
        assertEquals("10033007.1075840046962", m.getId());
        assertEquals(parser.sdf.parse("Thu, 29 Nov 2001 20:11:14 -0800 (PST)").getTime(), m.getTimestamp());
        
        assertEquals("slinger-r", m.getMailbox());
        assertEquals("inbox", m.getFolder());
        assertEquals("28.", m.getFileName());
        
        assertEquals("david.steiner@enron.com", m.getFrom());
        assertEquals("center.dl-portland@enron.com", m.getToAsString());
        assertEquals("Paul Kane Resigns", m.getSubject());
        assertNull(m.getCc());
        assertNull(m.getBcc());
        
        assertEquals(37, m.getBodyTokens().size());
        assertEquals(800, m.getRawBytes().length);
    }
}
